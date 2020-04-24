use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::time::Instant;

use indexmap::IndexMap;
use log::error;
use meilisearch_core::Filter;
use meilisearch_core::criterion::*;
use meilisearch_core::settings::RankingRule;
use meilisearch_core::{Highlight, Index, MainT, RankedMap};
use meilisearch_schema::{FieldId, Schema};
use meilisearch_tokenizer::is_cjk;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use siphasher::sip::SipHasher;

use crate::error::ResponseError;

pub trait IndexSearchExt {
    fn new_search(&self, query: String) -> SearchBuilder;
}

impl IndexSearchExt for Index {
    fn new_search(&self, query: String) -> SearchBuilder {
        SearchBuilder {
            index: self,
            query,
            offset: 0,
            limit: 20,
            attributes_to_crop: None,
            attributes_to_retrieve: None,
            attributes_to_highlight: None,
            filters: None,
            matches: false,
        }
    }
}

pub struct SearchBuilder<'a> {
    index: &'a Index,
    query: String,
    offset: usize,
    limit: usize,
    attributes_to_crop: Option<HashMap<String, usize>>,
    attributes_to_retrieve: Option<HashSet<String>>,
    attributes_to_highlight: Option<HashSet<String>>,
    filters: Option<String>,
    matches: bool,
}

impl<'a> SearchBuilder<'a> {
    pub fn offset(&mut self, value: usize) -> &SearchBuilder {
        self.offset = value;
        self
    }

    pub fn limit(&mut self, value: usize) -> &SearchBuilder {
        self.limit = value;
        self
    }

    pub fn attributes_to_crop(&mut self, value: HashMap<String, usize>) -> &SearchBuilder {
        self.attributes_to_crop = Some(value);
        self
    }

    pub fn attributes_to_retrieve(&mut self, value: HashSet<String>) -> &SearchBuilder {
        self.attributes_to_retrieve = Some(value);
        self
    }

    pub fn add_retrievable_field(&mut self, value: String) -> &SearchBuilder {
        let attributes_to_retrieve = self.attributes_to_retrieve.get_or_insert(HashSet::new());
        attributes_to_retrieve.insert(value);
        self
    }

    pub fn attributes_to_highlight(&mut self, value: HashSet<String>) -> &SearchBuilder {
        self.attributes_to_highlight = Some(value);
        self
    }

    pub fn filters(&mut self, value: String) -> &SearchBuilder {
        self.filters = Some(value);
        self
    }

    pub fn get_matches(&mut self) -> &SearchBuilder {
        self.matches = true;
        self
    }

    pub fn search(&self, reader: &heed::RoTxn<MainT>) -> Result<SearchResult, ResponseError> {
        let schema = self
            .index
            .main
            .schema(reader)?
            .ok_or(ResponseError::internal("missing schema"))?;

        let ranked_map = self.index.main.ranked_map(reader)?.unwrap_or_default();

        // Change criteria
        let mut query_builder = match self.get_criteria(reader, &ranked_map, &schema)? {
            Some(criteria) => self.index.query_builder_with_criteria(criteria),
            None => self.index.query_builder(),
        };

        if let Some(filter_expression) = &self.filters {
            let filter = Filter::parse(filter_expression, &schema)?;
            query_builder.with_filter(move |id| {
                let index = &self.index;
                let reader = &reader;
                let filter = &filter;
                match filter.test(reader, index, id) {
                    Ok(res) => res,
                    Err(e) => {
                        log::warn!("unexpected error during filtering: {}", e);
                        false
                    }
                }
            });
        }

        if let Some(field) = self.index.main.distinct_attribute(reader)? {
            if let Some(field_id) = schema.id(&field) {
                query_builder.with_distinct(1, move |id| {
                    match self.index.document_attribute_bytes(reader, id, field_id) {
                        Ok(Some(bytes)) => {
                            let mut s = SipHasher::new();
                            bytes.hash(&mut s);
                            Some(s.finish())
                        }
                        _ => None,
                    }
                });
            }
        }

        let start = Instant::now();
        let result = query_builder.query(reader, &self.query, self.offset..(self.offset + self.limit));
        let (docs, nb_hits) = result.map_err(ResponseError::search_documents)?;
        let time_ms = start.elapsed().as_millis() as usize;

        let mut all_attributes: HashSet<&str> = HashSet::new();
        let mut all_formatted: HashSet<&str> = HashSet::new();

        match &self.attributes_to_retrieve {
            Some(to_retrieve) => {
                all_attributes.extend(to_retrieve.iter().map(String::as_str));

                if let Some(to_highlight) = &self.attributes_to_highlight {
                    all_formatted.extend(to_highlight.iter().map(String::as_str));
                }

                if let Some(to_crop) = &self.attributes_to_crop {
                    all_formatted.extend(to_crop.keys().map(String::as_str));
                }

                all_attributes.extend(&all_formatted);
            },
            None => {
                all_attributes.extend(schema.displayed_name());
                // If we specified at least one attribute to highlight or crop then
                // all available attributes will be returned in the _formatted field.
                if self.attributes_to_highlight.is_some() || self.attributes_to_crop.is_some() {
                    all_formatted.extend(all_attributes.iter().cloned());
                }
            },
        }

        let mut hits = Vec::with_capacity(self.limit);
        for doc in docs {
            // retrieve the content of document in kv store
            let attributes: Option<HashSet<&str>> = self
                .attributes_to_retrieve
                .as_ref()
                .map(|a| a.iter().map(|a| a.as_str()).collect());

            let mut document: IndexMap<String, Value> = self
                .index
                .document(reader, attributes.as_ref(), doc.id)
                .map_err(|e| ResponseError::retrieve_document(doc.id.0, e))?
                .ok_or(ResponseError::internal(
                    "Impossible to retrieve the document; Corrupted data",
                ))?;

            let mut formatted = document.iter()
                .filter(|(key, _)| all_formatted.contains(key.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            let mut matches = doc.highlights.clone();

            // Crops fields if needed
            if let Some(fields) = &self.attributes_to_crop {
                crop_document(&mut formatted, &mut matches, &schema, fields);
            }

            // Transform to readable matches
            if let Some(attributes_to_highlight) = &self.attributes_to_highlight {
                let matches = calculate_matches(
                    matches.clone(),
                    self.attributes_to_highlight.clone(),
                    &schema,
                );
                formatted = calculate_highlights(&formatted, &matches, attributes_to_highlight);
            }

            let matches_info = if self.matches {
                Some(calculate_matches(matches, self.attributes_to_retrieve.clone(), &schema))
            } else {
                None
            };

            if let Some(attributes_to_retrieve) = &self.attributes_to_retrieve {
                document.retain(|key, _| attributes_to_retrieve.contains(&key.to_string()))
            }

            let hit = SearchHit {
                document,
                formatted,
                matches_info,
            };

            hits.push(hit);
        }

        let results = SearchResult {
            hits,
            offset: self.offset,
            limit: self.limit,
            nb_hits,
            exhaustive_nb_hits: false,
            processing_time_ms: time_ms,
            query: self.query.to_string(),
        };

        Ok(results)
    }

    pub fn get_criteria(
        &self,
        reader: &heed::RoTxn<MainT>,
        ranked_map: &'a RankedMap,
        schema: &Schema,
    ) -> Result<Option<Criteria<'a>>, ResponseError> {
        let ranking_rules = self.index.main.ranking_rules(reader)?;

        if let Some(ranking_rules) = ranking_rules {
            let mut builder = CriteriaBuilder::with_capacity(7 + ranking_rules.len());
            for rule in ranking_rules {
                match rule {
                    RankingRule::Typo => builder.push(Typo),
                    RankingRule::Words => builder.push(Words),
                    RankingRule::Proximity => builder.push(Proximity),
                    RankingRule::Attribute => builder.push(Attribute),
                    RankingRule::WordsPosition => builder.push(WordsPosition),
                    RankingRule::Exactness => builder.push(Exactness),
                    RankingRule::Asc(field) => {
                        match SortByAttr::lower_is_better(&ranked_map, &schema, &field) {
                            Ok(rule) => builder.push(rule),
                            Err(err) => error!("Error during criteria builder; {:?}", err),
                        }
                    }
                    RankingRule::Desc(field) => {
                        match SortByAttr::higher_is_better(&ranked_map, &schema, &field) {
                            Ok(rule) => builder.push(rule),
                            Err(err) => error!("Error during criteria builder; {:?}", err),
                        }
                    }
                }
            }
            builder.push(DocumentId);
            return Ok(Some(builder.build()));
        }

        Ok(None)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MatchPosition {
    pub start: usize,
    pub length: usize,
}

impl Ord for MatchPosition {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.start.cmp(&other.start) {
            Ordering::Equal => self.length.cmp(&other.length),
            _ => self.start.cmp(&other.start),
        }
    }
}

pub type HighlightInfos = HashMap<String, Value>;
pub type MatchesInfos = HashMap<String, Vec<MatchPosition>>;
// pub type RankingInfos = HashMap<String, u64>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    #[serde(flatten)]
    pub document: IndexMap<String, Value>,
    #[serde(rename = "_formatted", skip_serializing_if = "IndexMap::is_empty")]
    pub formatted: IndexMap<String, Value>,
    #[serde(rename = "_matchesInfo", skip_serializing_if = "Option::is_none")]
    pub matches_info: Option<MatchesInfos>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub offset: usize,
    pub limit: usize,
    pub nb_hits: usize,
    pub exhaustive_nb_hits: bool,
    pub processing_time_ms: usize,
    pub query: String,
}

/// returns the start index and the length on the crop.
fn aligned_crop(text: &str, match_index: usize, context: usize) -> (usize, usize) {
    let is_word_component = |c: &char| c.is_alphanumeric() && !is_cjk(*c);

    let word_end_index = |mut index| {
        if text.chars().nth(index - 1).map_or(false, |c| is_word_component(&c)) {
            index += text.chars().skip(index).take_while(is_word_component).count();
        }
        index
    };

    if context == 0 {
        // count need to be at least 1 for cjk queries to return something
        return (match_index, 1 + text.chars().skip(match_index).take_while(is_word_component).count());
    }
    let start = match match_index.saturating_sub(context) {
        n if n == 0 => n,
        n => word_end_index(n)
    };
    let end = word_end_index(start + 2 * context);

    (start, end - start)
}

fn crop_text(
    text: &str,
    matches: impl IntoIterator<Item = Highlight>,
    context: usize,
) -> (String, Vec<Highlight>) {
    let mut matches = matches.into_iter().peekable();

    let char_index = matches.peek().map(|m| m.char_index as usize).unwrap_or(0);
    let (start, count) = aligned_crop(text, char_index, context);

    //TODO do something about the double allocation
    let text = text.chars().skip(start).take(count).collect::<String>().trim().to_string();

    // update matches index to match the new cropped text
    let matches = matches
        .take_while(|m| (m.char_index as usize) + (m.char_length as usize) <= start + (context * 2))
        .map(|match_| Highlight {
            char_index: match_.char_index - start as u16,
            ..match_
        })
        .collect();

    (text, matches)
}

fn crop_document(
    document: &mut IndexMap<String, Value>,
    matches: &mut Vec<Highlight>,
    schema: &Schema,
    fields: &HashMap<String, usize>,
) {
    matches.sort_unstable_by_key(|m| (m.char_index, m.char_length));

    for (field, length) in fields {
        let attribute = match schema.id(field) {
            Some(attribute) => attribute,
            None => continue,
        };

        let selected_matches = matches
            .iter()
            .filter(|m| FieldId::new(m.attribute) == attribute)
            .cloned();

        if let Some(Value::String(ref mut original_text)) = document.get_mut(field) {
            let (cropped_text, cropped_matches) =
                crop_text(original_text, selected_matches, *length);

            *original_text = cropped_text;

            matches.retain(|m| FieldId::new(m.attribute) != attribute);
            matches.extend_from_slice(&cropped_matches);
        }
    }
}

fn calculate_matches(
    matches: Vec<Highlight>,
    attributes_to_retrieve: Option<HashSet<String>>,
    schema: &Schema,
) -> MatchesInfos {
    let mut matches_result: HashMap<String, Vec<MatchPosition>> = HashMap::new();
    for m in matches.iter() {
        if let Some(attribute) = schema.name(FieldId::new(m.attribute)) {
            if let Some(attributes_to_retrieve) = attributes_to_retrieve.clone() {
                if !attributes_to_retrieve.contains(attribute) {
                    continue;
                }
            }
            if !schema.displayed_name().contains(attribute) {
                continue;
            }
            if let Some(pos) = matches_result.get_mut(attribute) {
                pos.push(MatchPosition {
                    start: m.char_index as usize,
                    length: m.char_length as usize,
                });
            } else {
                let mut positions = Vec::new();
                positions.push(MatchPosition {
                    start: m.char_index as usize,
                    length: m.char_length as usize,
                });
                matches_result.insert(attribute.to_string(), positions);
            }
        }
    }
    for (_, val) in matches_result.iter_mut() {
        val.sort_unstable();
        val.dedup();
    }
    matches_result
}

fn calculate_highlights(
    document: &IndexMap<String, Value>,
    matches: &MatchesInfos,
    attributes_to_highlight: &HashSet<String>,
) -> IndexMap<String, Value> {
    let mut highlight_result = document.clone();

    for (attribute, matches) in matches.iter() {
        if attributes_to_highlight.contains(attribute) {
            if let Some(Value::String(value)) = document.get(attribute) {
                let value: Vec<_> = value.chars().collect();
                let mut highlighted_value = String::new();
                let mut index = 0;
                for m in matches {
                    if m.start >= index {
                        let before = value.get(index..m.start);
                        let highlighted = value.get(m.start..(m.start + m.length));
                        if let (Some(before), Some(highlighted)) = (before, highlighted) {
                            highlighted_value.extend(before);
                            highlighted_value.push_str("<em>");
                            highlighted_value.extend(highlighted);
                            highlighted_value.push_str("</em>");
                            index = m.start + m.length;
                        } else {
                            error!("value: {:?}; index: {:?}, match: {:?}", value, index, m);
                        }
                    }
                }
                highlighted_value.extend(value[index..].iter());
                highlight_result.insert(attribute.to_string(), Value::String(highlighted_value));
            };
        }
    }

    highlight_result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_crops() {
        let text = r#"En ce début de trentième millénaire, l'Empire n'a jamais été aussi puissant, aussi étendu à travers toute la galaxie. C'est dans sa capitale, Trantor, que l'éminent savant Hari Seldon invente la psychohistoire, une science toute nouvelle, à base de psychologie et de mathématiques, qui lui permet de prédire l'avenir... C'est-à-dire l'effondrement de l'Empire d'ici cinq siècles et au-delà, trente mille années de chaos et de ténèbres. Pour empêcher cette catastrophe et sauver la civilisation, Seldon crée la Fondation."#;

        // simple test
        let (start, length) = aligned_crop(&text, 6, 2);
        let cropped =  text.chars().skip(start).take(length).collect::<String>().trim().to_string();
        assert_eq!("début", cropped);

        // first word test
        let (start, length) = aligned_crop(&text, 0, 1);
        let cropped =  text.chars().skip(start).take(length).collect::<String>().trim().to_string();
        assert_eq!("En", cropped);
        // last word test
        let (start, length) = aligned_crop(&text, 510, 2);
        let cropped =  text.chars().skip(start).take(length).collect::<String>().trim().to_string();
        assert_eq!("Fondation", cropped);

        // CJK tests
        let text = "this isのス foo myタイリ test";

        // mixed charset
        let (start, length) = aligned_crop(&text, 5, 3);
        let cropped =  text.chars().skip(start).take(length).collect::<String>().trim().to_string();
        assert_eq!("isのス", cropped);

        // split regular word / CJK word, no space
        let (start, length) = aligned_crop(&text, 7, 1);
        let cropped =  text.chars().skip(start).take(length).collect::<String>().trim().to_string();
        assert_eq!("のス", cropped);

    }

    #[test]
    fn calculate_highlights() {
        let data = r#"{
            "title": "Fondation (Isaac ASIMOV)",
            "description": "En ce début de trentième millénaire, l'Empire n'a jamais été aussi puissant, aussi étendu à travers toute la galaxie. C'est dans sa capitale, Trantor, que l'éminent savant Hari Seldon invente la psychohistoire, une science toute nouvelle, à base de psychologie et de mathématiques, qui lui permet de prédire l'avenir... C'est-à-dire l'effondrement de l'Empire d'ici cinq siècles et au-delà, trente mille années de chaos et de ténèbres. Pour empêcher cette catastrophe et sauver la civilisation, Seldon crée la Fondation."
        }"#;

        let document: IndexMap<String, Value> = serde_json::from_str(data).unwrap();
        let mut attributes_to_highlight = HashSet::new();
        attributes_to_highlight.insert("title".to_string());
        attributes_to_highlight.insert("description".to_string());

        let mut matches = HashMap::new();

        let mut m = Vec::new();
        m.push(MatchPosition {
            start: 0,
            length: 9,
        });
        matches.insert("title".to_string(), m);

        let mut m = Vec::new();
        m.push(MatchPosition {
            start: 510,
            length: 9,
        });
        matches.insert("description".to_string(), m);
        let result = super::calculate_highlights(&document, &matches, &attributes_to_highlight);

        let mut result_expected = IndexMap::new();
        result_expected.insert(
            "title".to_string(),
            Value::String("<em>Fondation</em> (Isaac ASIMOV)".to_string()),
        );
        result_expected.insert("description".to_string(), Value::String("En ce début de trentième millénaire, l'Empire n'a jamais été aussi puissant, aussi étendu à travers toute la galaxie. C'est dans sa capitale, Trantor, que l'éminent savant Hari Seldon invente la psychohistoire, une science toute nouvelle, à base de psychologie et de mathématiques, qui lui permet de prédire l'avenir... C'est-à-dire l'effondrement de l'Empire d'ici cinq siècles et au-delà, trente mille années de chaos et de ténèbres. Pour empêcher cette catastrophe et sauver la civilisation, Seldon crée la <em>Fondation</em>.".to_string()));

        assert_eq!(result, result_expected);
    }
}
