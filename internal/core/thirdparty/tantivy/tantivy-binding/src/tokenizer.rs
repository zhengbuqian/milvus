use lazy_static::lazy_static;
use log::{info, warn};
use std::collections::HashMap;
use tantivy::tokenizer::{TextAnalyzer, TokenizerManager, LowerCaser, StopWordFilter, SimpleTokenizer, Stemmer, Language};
use crate::log::init_log;

lazy_static! {
    static ref DEFAULT_TOKENIZER_MANAGER: TokenizerManager = {
        let mut manager = TokenizerManager::default();
        let en_lower_stop_stem = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .filter(StopWordFilter::new(Language::English).unwrap())
            .filter(Stemmer::new(Language::English))
            .build();
        manager.register("default", en_lower_stop_stem);
        let en_lower_stop = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .filter(StopWordFilter::new(Language::English).unwrap())
            .build();
        manager.register("en_lower_stop", en_lower_stop);
        let en_stop = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(StopWordFilter::new(Language::English).unwrap())
            .build();
        manager.register("en_stop", en_stop);
        manager
    };
}

pub(crate) fn default_tokenizer() -> TextAnalyzer {
    DEFAULT_TOKENIZER_MANAGER.get("default").unwrap()
}

fn en_stop_tokenizer() -> TextAnalyzer {
    DEFAULT_TOKENIZER_MANAGER.get("en_stop").unwrap()
}

fn en_lower_stop_tokenizer() -> TextAnalyzer {
    DEFAULT_TOKENIZER_MANAGER.get("en_lower_stop").unwrap()
}

fn jieba_tokenizer() -> TextAnalyzer {
    tantivy_jieba::JiebaTokenizer {}.into()
}

pub(crate) fn create_tokenizer(params: &HashMap<String, String>) -> Option<TextAnalyzer> {
    init_log();

    match params.get("tokenizer") {
        Some(tokenizer_name) => match tokenizer_name.as_str() {
            "default" => {
                Some(default_tokenizer())
            }
            "jieba" => {
                Some(jieba_tokenizer())
            }
            "en_stop" => {
                Some(en_stop_tokenizer())
            }
            "en_lower_stop" => {
                Some(en_lower_stop_tokenizer())
            }
            s => {
                warn!("unsupported tokenizer: {}", s);
                None
            }
        },
        None => {
            Some(default_tokenizer())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::tokenizer::create_tokenizer;

    #[test]
    fn test_create_tokenizer() {
        let mut params : HashMap<String, String> = HashMap::new();
        params.insert("tokenizer".parse().unwrap(), "jieba".parse().unwrap());

        let tokenizer = create_tokenizer(&params);
        assert!(tokenizer.is_some());
    }
}
