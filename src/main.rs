//! AWS Lambda Telegram bot that searches Wikipedia, renders article content for
//! Telegram HTML, and fetches slower metadata/media follow-ups in parallel.
//!
//! The file is intentionally single-module for Lambda packaging simplicity, but
//! the functions are grouped by boundary: webhook handling, Telegram sends,
//! Wikimedia clients, callback/keyboard builders, HTML extraction/rendering, and
//! media cleanup.

use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ego_tree::NodeRef;
use lambda_http::http::{Method, StatusCode};
use lambda_http::{Body, Error, IntoResponse, Request, Response, run, service_fn};
use regex::Regex;
use reqwest::Client;
use scraper::Node;
use scraper::{ElementRef, Html, Selector};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

const DEFAULT_SEARCH_LIMIT: usize = 10;
const MAX_SEARCH_LIMIT: usize = 20;
const DEFAULT_MESSAGE_CHAR_LIMIT: usize = 3900;
const DEFAULT_BIG_ARTICLE_CHAR_THRESHOLD: usize = 3900;
const DEFAULT_SMALL_ARTICLE_CHAR_THRESHOLD: usize = 1000;
const DEFAULT_METADATA_REVISION_LIMIT: usize = 10;
const DEFAULT_HTTP_TIMEOUT_SECONDS: u64 = 20;
const DEFAULT_RAM_CACHE_MAX_ENTRIES: usize = 512;
const TELEGRAM_MAX_BUTTON_TEXT: usize = 64;
const CATEGORY_BUTTON_LIMIT: usize = 12;
const CATEGORY_SEARCH_FETCH_LIMIT: usize = 500;
const NAV_TEMPLATE_LINK_LIMIT: usize = 30;
const DISAMBIGUATION_LINK_LIMIT: usize = 300;
const DISAMBIGUATION_BUTTON_PREFIX: &str = "🔀 ";
const ARTICLE_BODY_LINK_LIMIT: usize = 20;
const ARTICLE_TITLE_LOOKUP_LIMIT: usize = 50;
const MEDIA_FILE_LOOKUP_LIMIT: usize = 50;
const ARTICLE_IMAGE_GALLERY_LIMIT: usize = 10;
const ARTICLE_IMAGE_FALLBACK_THUMB_WIDTH: usize = 3840;
const MIN_ICON_FILTER_SIDE_PX: u64 = 160;
const INLINE_MESSAGE_CHAR_LIMIT: usize = 3900;
const INLINE_CACHE_TIME_SECONDS: u32 = 0;
const TABLE_CELL_MAX_CHARS: usize = 48;
const TELEGRAM_PRE_OPEN: &str = "<pre>";
const TELEGRAM_PRE_CLOSE: &str = "</pre>";
const WIKIDATA_ENTITY_BATCH_SIZE: usize = 50;
const WIKIDATA_PROPERTY_DISPLAY_LIMIT: usize = 80;
const WIKIDATA_VALUES_PER_PROPERTY_LIMIT: usize = 8;
const REPOSITORY_URL: &str = "https://github.com/vitaly-zdanevich/bot_telegram_wikipedia";
const WIKIDATA_API_URL: &str = "https://www.wikidata.org/w/api.php";

static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();
static RAM_CACHE: OnceLock<Mutex<RamCache>> = OnceLock::new();
static INFOBOX_TABLE_RE: OnceLock<Regex> = OnceLock::new();
static STYLE_SCRIPT_RE: OnceLock<Regex> = OnceLock::new();
static SUP_RE: OnceLock<Regex> = OnceLock::new();
static TAG_RE: OnceLock<Regex> = OnceLock::new();
static COORDINATE_DMS_RE: OnceLock<Regex> = OnceLock::new();

/// Starts the Lambda HTTP runtime and initializes structured logging.
#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "telegram_wikipedia_bot=info,tower_http=warn".into()),
        )
        .without_time()
        .init();

    run(service_fn(handler)).await
}

/// Handles Telegram webhook requests from the Lambda Function URL.
///
/// The handler authenticates Telegram's secret token, parses the update, and
/// intentionally returns `200 OK` after logging bot errors so Telegram does not
/// retry already-received updates.
async fn handler(request: Request) -> Result<impl IntoResponse, Error> {
    let config = Config::from_env()?;

    if request.method() != Method::POST {
        return Ok(response(StatusCode::OK, "ok"));
    }

    let actual_secret = request
        .headers()
        .get("x-telegram-bot-api-secret-token")
        .and_then(|value| value.to_str().ok());

    if actual_secret != Some(config.telegram_webhook_secret.as_str()) {
        warn!("rejected webhook with missing or invalid secret token");
        return Ok(response(StatusCode::UNAUTHORIZED, "unauthorized"));
    }

    let update = match parse_update(request.body()) {
        Ok(update) => update,
        Err(err) => {
            warn!(error = %err, "ignored invalid Telegram update");
            return Ok(response(StatusCode::OK, "ignored"));
        }
    };

    let app = App::new(config)?;
    if let Err(err) = app.handle_update(update).await {
        error!(error = %format_error_chain(&err), "failed to handle Telegram update");
    }

    Ok(response(StatusCode::OK, "ok"))
}

/// Builds a small plain-text Lambda response.
fn response(status: StatusCode, body: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Body::Text(body.to_string()))
        .expect("static response is valid")
}

/// Deserializes a Telegram update from the Lambda body variants we accept.
fn parse_update(body: &Body) -> Result<Update> {
    match body {
        Body::Text(text) => serde_json::from_str(text).context("failed to parse text body"),
        Body::Binary(bytes) => serde_json::from_slice(bytes).context("failed to parse binary body"),
        Body::Empty => bail!("empty body"),
        _ => bail!("unsupported request body type"),
    }
}

/// Flattens an anyhow error chain for concise structured log fields.
fn format_error_chain(err: &anyhow::Error) -> String {
    err.chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

#[derive(Clone)]
struct Config {
    telegram_bot_token: String,
    telegram_webhook_secret: String,
    allowed_telegram_user_ids: HashSet<i64>,
    default_wiki_language: String,
    favorite_categories: Vec<FavoriteCategory>,
    search_limit: usize,
    message_char_limit: usize,
    big_article_char_threshold: usize,
    small_article_char_threshold: usize,
    metadata_revision_limit: usize,
    http_timeout_seconds: u64,
    ram_cache_max_entries: usize,
    article_images_button_only: bool,
}

impl Config {
    /// Reads Lambda environment variables and normalizes the values used by the
    /// bot at runtime.
    fn from_env() -> Result<Self> {
        let default_wiki_language = normalize_language_code(
            &optional_env("DEFAULT_WIKI_LANGUAGE").unwrap_or_else(|| "en".to_string()),
        )
        .context("DEFAULT_WIKI_LANGUAGE must be a Wikipedia language code")?;

        let favorite_categories = parse_favorite_categories(
            optional_env("FAVORITE_CATEGORIES").as_deref(),
            &default_wiki_language,
        )?;

        Ok(Self {
            telegram_bot_token: required_env("TELEGRAM_BOT_TOKEN")?,
            telegram_webhook_secret: required_env("TELEGRAM_WEBHOOK_SECRET")?,
            allowed_telegram_user_ids: parse_allowed_telegram_user_ids()?,
            default_wiki_language,
            favorite_categories,
            search_limit: parse_env("SEARCH_LIMIT", DEFAULT_SEARCH_LIMIT)?.min(MAX_SEARCH_LIMIT),
            message_char_limit: parse_env(
                "TELEGRAM_MESSAGE_CHAR_LIMIT",
                DEFAULT_MESSAGE_CHAR_LIMIT,
            )?,
            big_article_char_threshold: parse_env(
                "BIG_ARTICLE_CHAR_THRESHOLD",
                DEFAULT_BIG_ARTICLE_CHAR_THRESHOLD,
            )?,
            small_article_char_threshold: parse_env(
                "SMALL_ARTICLE_CHAR_THRESHOLD",
                DEFAULT_SMALL_ARTICLE_CHAR_THRESHOLD,
            )?,
            metadata_revision_limit: parse_env(
                "METADATA_REVISION_LIMIT",
                DEFAULT_METADATA_REVISION_LIMIT,
            )?
            .min(50),
            http_timeout_seconds: parse_env(
                "WIKIPEDIA_HTTP_TIMEOUT_SECONDS",
                DEFAULT_HTTP_TIMEOUT_SECONDS,
            )?,
            ram_cache_max_entries: parse_env(
                "RAM_CACHE_MAX_ENTRIES",
                DEFAULT_RAM_CACHE_MAX_ENTRIES,
            )?,
            article_images_button_only: parse_bool_env("ARTICLE_IMAGES_BUTTON_ONLY", false)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FavoriteCategory {
    language: String,
    title: String,
}

#[derive(Clone)]
struct App {
    config: Config,
    http: Client,
    wiki: WikiClient,
}

impl App {
    /// Creates the application facade and shared HTTP client used for Telegram
    /// and Wikimedia requests.
    fn new(config: Config) -> Result<Self> {
        let timeout = Duration::from_secs(config.http_timeout_seconds);
        let http = HTTP_CLIENT
            .get_or_init(|| {
                Client::builder()
                    .timeout(timeout)
                    .user_agent(concat!(
                        env!("CARGO_PKG_NAME"),
                        "/",
                        env!("CARGO_PKG_VERSION"),
                        " (https://github.com/vitaly-zdanevich/bot_telegram_wikipedia)"
                    ))
                    .build()
                    .expect("HTTP client config is valid")
            })
            .clone();

        Ok(Self {
            wiki: WikiClient::new(http.clone(), config.ram_cache_max_entries),
            http,
            config,
        })
    }

    /// Routes one Telegram update to the matching message, callback, or inline
    /// query handler.
    async fn handle_update(&self, update: Update) -> Result<()> {
        if let Some(callback) = update.callback_query {
            return self.handle_callback(callback).await;
        }

        if let Some(inline_query) = update.inline_query {
            return self.handle_inline_query(inline_query).await;
        }

        if let Some(message) = update.message {
            return self.handle_message(message).await;
        }

        info!(
            update_id = update.update_id,
            "ignored unsupported update type"
        );
        Ok(())
    }

    /// Handles normal chat messages: commands, category aliases, and Wikipedia
    /// search queries.
    async fn handle_message(&self, message: Message) -> Result<()> {
        let chat_id = message.chat.id;
        if !self.is_allowed_user(message.from.as_ref().map(|user| user.id)) {
            warn!(
                chat_id,
                user_id = message.from.as_ref().map(|user| user.id),
                "ignored message from unauthorized Telegram user"
            );
            return Ok(());
        }

        let Some(text) = message
            .text
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(());
        };

        if text.starts_with("/start") || text.starts_with("/help") {
            return self.send_help(chat_id).await;
        }

        if let Some(category_request) =
            parse_category_command(text, &self.config.default_wiki_language)?
        {
            return self
                .send_category_search(chat_id, &category_request.language, &category_request.title)
                .await;
        }

        if text.starts_with('/') {
            return self.send_help(chat_id).await;
        }

        let search = parse_search_request(text, &self.config.default_wiki_language)?;
        self.send_search_results(chat_id, &search.language, &search.query)
            .await
    }

    /// Handles Telegram callback button payloads produced by this bot.
    async fn handle_callback(&self, callback: CallbackQuery) -> Result<()> {
        if !self.is_allowed_user(Some(callback.from.id)) {
            warn!(
                user_id = callback.from.id,
                "ignored callback from unauthorized Telegram user"
            );
            self.answer_callback_query(&callback.id, "Not allowed.", true)
                .await?;
            return Ok(());
        }

        let Some(message) = callback.message else {
            self.answer_callback_query(&callback.id, "Message is not available.", true)
                .await?;
            return Ok(());
        };

        let chat_id = message.chat.id;
        let data = callback.data.as_deref().unwrap_or_default();
        match parse_callback_data(data, &self.config.favorite_categories) {
            Ok(CallbackAction::Article { language, page_id }) => {
                self.answer_callback_query(&callback.id, "Opening article...", false)
                    .await?;
                self.send_article(chat_id, language, page_id).await?;
            }
            Ok(CallbackAction::Category { language, title }) => {
                self.answer_callback_query(&callback.id, "Loading category...", false)
                    .await?;
                self.send_category_members(chat_id, &language, &title)
                    .await?;
            }
            Ok(CallbackAction::CategoryPage {
                language,
                page_id,
                kind,
                page_index,
                total_pages,
            }) => {
                self.answer_callback_query(&callback.id, "Loading category page...", false)
                    .await?;
                self.send_category_page(chat_id, &language, page_id, kind, page_index, total_pages)
                    .await?;
            }
            Ok(CallbackAction::ArticleCategory {
                language,
                page_id,
                index,
            }) => {
                self.answer_callback_query(&callback.id, "Loading category...", false)
                    .await?;
                let categories = self.wiki.article_categories(&language, page_id).await?;
                let Some(category) = categories.get(index) else {
                    self.send_message(
                        chat_id,
                        "This category button is no longer available.",
                        None,
                    )
                    .await?;
                    return Ok(());
                };
                self.send_category_members(chat_id, &language, &category.title)
                    .await?;
            }
            Ok(CallbackAction::Wikidata { language, item }) => {
                self.answer_callback_query(&callback.id, "Loading Wikidata...", false)
                    .await?;
                self.send_wikidata(chat_id, &language, &item).await?;
            }
            Ok(CallbackAction::Images { language, page_id }) => {
                self.answer_callback_query(&callback.id, "Loading images...", false)
                    .await?;
                self.send_article_images(chat_id, &language, page_id)
                    .await?;
            }
            Err(err) => {
                warn!(error = %err, data, "ignored invalid callback data");
                self.answer_callback_query(&callback.id, "This button is expired.", true)
                    .await?;
            }
        }

        Ok(())
    }

    /// Handles inline mode by returning article cards that can be inserted into
    /// any chat.
    async fn handle_inline_query(&self, inline_query: InlineQuery) -> Result<()> {
        if !self.is_allowed_user(Some(inline_query.from.id)) {
            warn!(
                user_id = inline_query.from.id,
                "ignored inline query from unauthorized Telegram user"
            );
            self.answer_inline_query(&inline_query.id, Vec::new(), 0, true)
                .await?;
            return Ok(());
        }

        let query = inline_query.query.trim();
        if query.is_empty() {
            self.answer_inline_query(&inline_query.id, Vec::new(), 1, true)
                .await?;
            return Ok(());
        }

        let (language, results) = if let Some(category_request) =
            parse_category_command(query, &self.config.default_wiki_language)?
        {
            let results = self
                .wiki
                .category_articles(
                    &category_request.language,
                    &category_request.title,
                    self.config.search_limit,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to load inline category {} from {}.wikipedia.org",
                        category_request.title, category_request.language
                    )
                })?;
            (category_request.language, results)
        } else {
            let search = parse_search_request(query, &self.config.default_wiki_language)?;
            let results = self
                .wiki
                .inline_search(&search.language, &search.query, self.config.search_limit)
                .await
                .with_context(|| {
                    format!(
                        "failed to search inline query in {}.wikipedia.org",
                        search.language
                    )
                })?;
            (search.language, results)
        };

        self.answer_inline_query(
            &inline_query.id,
            inline_query_results(&language, &results),
            INLINE_CACHE_TIME_SECONDS,
            false,
        )
        .await
    }

    /// Checks the optional allow-list for public or restricted bot operation.
    fn is_allowed_user(&self, user_id: Option<i64>) -> bool {
        self.config.allowed_telegram_user_ids.is_empty()
            || user_id.is_some_and(|id| self.config.allowed_telegram_user_ids.contains(&id))
    }

    /// Sends usage text and configured favorite category buttons.
    async fn send_help(&self, chat_id: i64) -> Result<()> {
        let text = format!(
            "Send a Wikipedia search query.\n\nInline mode also works: type this bot's username followed by a query in any chat.\n\nUse lang:&lt;code&gt; before a query for a specific Wikipedia, for example <b>lang:be Беларусь</b>.\n\nFor Latin text I use {} by default. For scripts such as Cyrillic, Georgian, Arabic, Hebrew, Greek, Japanese, Korean, Chinese, Hindi, and Thai I pick the matching Wikipedia automatically.\n\nUse /category lang:Category name to search matching categories, then press a category button to show subcategories and newest pages added to it. Category aliases also work: Category:Physics, Category: Physics, Category Physics, c Physics, Категория Физика, Категория:Физика, Катэгорыя Навука, Катэгорыя:Навука, к Физика, к be:Навука.\n\nSource code: {}",
            self.config.default_wiki_language, REPOSITORY_URL
        );

        self.send_html_message(
            chat_id,
            &text,
            favorite_categories_keyboard(&self.config.favorite_categories),
        )
        .await
    }

    /// Searches Wikipedia and sends article result buttons.
    async fn send_search_results(&self, chat_id: i64, language: &str, query: &str) -> Result<()> {
        self.send_chat_action(chat_id, "typing").await?;

        let results = self
            .wiki
            .search(language, query, self.config.search_limit)
            .await
            .with_context(|| format!("failed to search {language}.wikipedia.org"))?;

        if results.is_empty() {
            self.send_message(
                chat_id,
                &format!("No results in {language}.wikipedia.org for: {query}"),
                favorite_categories_keyboard(&self.config.favorite_categories),
            )
            .await?;
            return Ok(());
        }

        let reply_markup = json!({
            "inline_keyboard": article_results_keyboard(
                language,
                &results,
                self.config.big_article_char_threshold,
                self.config.small_article_char_threshold,
                false
            )
        });

        self.send_message(
            chat_id,
            &format!("Results from {language}.wikipedia.org for: {query}"),
            Some(reply_markup),
        )
        .await
    }

    /// Searches matching category pages and sends category callback buttons.
    async fn send_category_search(
        &self,
        chat_id: i64,
        language: &str,
        title_or_query: &str,
    ) -> Result<()> {
        self.send_chat_action(chat_id, "typing").await?;

        let query = category_display_title(title_or_query);
        let categories = self
            .wiki
            .search_categories(language, &query, self.config.search_limit)
            .await
            .with_context(|| {
                format!("failed to search categories for {query} on {language}.wikipedia.org")
            })?;

        if categories.is_empty() {
            self.send_message(
                chat_id,
                &format!("No matching categories in {language}.wikipedia.org for: {query}"),
                favorite_categories_keyboard(&self.config.favorite_categories),
            )
            .await?;
            return Ok(());
        }

        self.send_html_message(
            chat_id,
            &render_category_search_html(language, &query, &categories),
            category_search_keyboard(language, &categories),
        )
        .await
    }

    /// Sends a category overview, parent categories, subcategories, and newest
    /// article pages.
    async fn send_category_members(&self, chat_id: i64, language: &str, title: &str) -> Result<()> {
        self.send_chat_action(chat_id, "typing").await?;

        let articles = async {
            self.wiki
                .category_articles_page(language, title, self.config.search_limit, 0)
                .await
                .with_context(|| {
                    format!(
                        "failed to load category articles for {title} from {language}.wikipedia.org"
                    )
                })
        };
        let subcategories = async {
            self.wiki
                .category_subcategories_page(language, title, self.config.search_limit, 0)
                .await
                .with_context(|| {
                    format!(
                        "failed to load subcategories for {title} from {language}.wikipedia.org"
                    )
                })
        };
        let overview = async {
            self.wiki
                .category_overview(language, title)
                .await
                .with_context(|| {
                    format!(
                        "failed to load category overview for {title} from {language}.wikipedia.org"
                    )
                })
        };
        let (articles, subcategories, overview) =
            tokio::try_join!(articles, subcategories, overview)?;
        info!(
            language,
            title,
            articles = articles.articles.len(),
            subcategories = subcategories.categories.len(),
            "loaded category members"
        );

        if let Some(description) = overview
            .description
            .as_deref()
            .map(str::trim)
            .filter(|description| !description.is_empty())
        {
            for part in render_category_description_html_parts(
                language,
                title,
                description,
                self.config.message_char_limit,
            ) {
                self.send_html_message(chat_id, &part, None).await?;
            }
        }

        if !overview.parent_categories.is_empty() {
            self.send_html_message(
                chat_id,
                &render_parent_categories_html(language, title, &overview.parent_categories),
                category_search_keyboard(language, &overview.parent_categories),
            )
            .await?;
        }

        if articles.articles.is_empty() && subcategories.categories.is_empty() {
            self.send_message(
                chat_id,
                &format!("No category members found in {title} on {language}.wikipedia.org."),
                favorite_categories_keyboard(&self.config.favorite_categories),
            )
            .await?;
            return Ok(());
        }

        if !subcategories.categories.is_empty() {
            let total_pages =
                category_total_pages(overview.subcategory_count, self.config.search_limit);
            self.send_html_message(
                chat_id,
                &render_category_subcategories_page_heading_html(
                    language,
                    title,
                    subcategories.page_index,
                    total_pages,
                ),
                category_subcategories_keyboard_with_pagination(
                    language,
                    &subcategories.categories,
                    overview.page_id.map(|page_id| CategoryPagination {
                        language,
                        page_id,
                        kind: CategoryPageKind::Subcategories,
                        page_index: subcategories.page_index,
                        total_pages,
                        has_next: subcategories.has_next,
                    }),
                ),
            )
            .await?;
        }

        if !articles.articles.is_empty() {
            let total_pages =
                category_total_pages(overview.article_count, self.config.search_limit);
            let reply_markup = category_articles_keyboard(
                language,
                &articles.articles,
                self.config.big_article_char_threshold,
                self.config.small_article_char_threshold,
                articles.articles.len() == 1,
                overview.page_id.map(|page_id| CategoryPagination {
                    language,
                    page_id,
                    kind: CategoryPageKind::Articles,
                    page_index: articles.page_index,
                    total_pages,
                    has_next: articles.has_next,
                }),
            );

            self.send_html_message(
                chat_id,
                &render_category_articles_page_heading_html(
                    language,
                    title,
                    articles.page_index,
                    total_pages,
                ),
                reply_markup,
            )
            .await?;
        }

        Ok(())
    }

    /// Sends a paginated category article or subcategory page.
    async fn send_category_page(
        &self,
        chat_id: i64,
        language: &str,
        page_id: u64,
        kind: CategoryPageKind,
        page_index: usize,
        total_pages: Option<usize>,
    ) -> Result<()> {
        self.send_chat_action(chat_id, "typing").await?;

        let title = self
            .wiki
            .category_title_by_page_id(language, page_id)
            .await
            .with_context(|| {
                format!(
                    "failed to resolve category page_id={page_id} from {language}.wikipedia.org"
                )
            })?;

        match kind {
            CategoryPageKind::Articles => {
                let page = self
                    .wiki
                    .category_articles_page(language, &title, self.config.search_limit, page_index)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to load category article page {page_index} for {title} from {language}.wikipedia.org"
                        )
                    })?;
                if page.articles.is_empty() {
                    self.send_message(chat_id, "No articles found on this category page.", None)
                        .await?;
                    return Ok(());
                }

                self.send_html_message(
                    chat_id,
                    &render_category_articles_page_heading_html(
                        language,
                        &title,
                        page.page_index,
                        total_pages,
                    ),
                    category_articles_keyboard(
                        language,
                        &page.articles,
                        self.config.big_article_char_threshold,
                        self.config.small_article_char_threshold,
                        page.articles.len() == 1,
                        Some(CategoryPagination {
                            language,
                            page_id,
                            kind,
                            page_index: page.page_index,
                            total_pages,
                            has_next: page.has_next,
                        }),
                    ),
                )
                .await?;
            }
            CategoryPageKind::Subcategories => {
                let page = self
                    .wiki
                    .category_subcategories_page(
                        language,
                        &title,
                        self.config.search_limit,
                        page_index,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "failed to load subcategory page {page_index} for {title} from {language}.wikipedia.org"
                        )
                    })?;
                if page.categories.is_empty() {
                    self.send_message(
                        chat_id,
                        "No subcategories found on this category page.",
                        None,
                    )
                    .await?;
                    return Ok(());
                }

                self.send_html_message(
                    chat_id,
                    &render_category_subcategories_page_heading_html(
                        language,
                        &title,
                        page.page_index,
                        total_pages,
                    ),
                    category_subcategories_keyboard_with_pagination(
                        language,
                        &page.categories,
                        Some(CategoryPagination {
                            language,
                            page_id,
                            kind,
                            page_index: page.page_index,
                            total_pages,
                            has_next: page.has_next,
                        }),
                    ),
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Sends an article body first, then slower follow-up data such as links,
    /// disambiguation groups, navigation, categories, metadata, and media.
    async fn send_article(&self, chat_id: i64, language: String, page_id: u64) -> Result<()> {
        self.send_chat_action(chat_id, "typing").await?;

        let article = match self
            .wiki
            .article_content(&language, page_id)
            .await
            .with_context(|| format!("failed to fetch article page_id={page_id} from {language}"))
        {
            Ok(article) => article,
            Err(err) => {
                warn!(error = %format_error_chain(&err), "failed to fetch article");
                self.send_message(chat_id, "Article fetch failed. Please try again.", None)
                    .await?;
                return Ok(());
            }
        };

        let mut followups = ArticleFollowupTasks::default();
        let inline_images = self
            .send_article_body_parts(chat_id, &language, page_id, &article, &mut followups)
            .await?;

        self.send_article_references(chat_id, &article).await?;
        self.send_article_followups(
            chat_id,
            &language,
            page_id,
            &article,
            followups,
            &inline_images,
        )
        .await?;

        Ok(())
    }

    /// Sends the already-rendered article body and starts follow-up network work
    /// only after Telegram has accepted the first text message.
    async fn send_article_body_parts(
        &self,
        chat_id: i64,
        language: &str,
        page_id: u64,
        article: &ArticleContent,
        followups: &mut ArticleFollowupTasks,
    ) -> Result<Vec<MediaItem>> {
        let parts = render_article_output_parts(language, article, self.config.message_char_limit);
        let mut inline_images = Vec::new();

        for part in parts {
            match part {
                ArticleOutputPart::Html(html) => {
                    self.send_html_message(chat_id, &html, None).await?;
                    if !followups.started {
                        *followups = self.start_article_followups(language, page_id, article);
                    }
                }
                ArticleOutputPart::Images(images) => {
                    if !self.config.article_images_button_only && !images.is_empty() {
                        self.send_image_gallery(chat_id, &images).await?;
                        inline_images.extend(images);
                    }
                }
            }
        }

        Ok(inline_images)
    }

    /// Starts all article follow-up fetches in parallel.
    fn start_article_followups(
        &self,
        language: &str,
        page_id: u64,
        article: &ArticleContent,
    ) -> ArticleFollowupTasks {
        let categories = Some(spawn_wiki_task({
            let wiki = self.wiki.clone();
            let language = language.to_string();
            async move { wiki.article_categories(&language, page_id).await }
        }));

        let body_links = (!article.body_links.is_empty()).then(|| {
            spawn_wiki_task({
                let wiki = self.wiki.clone();
                let language = language.to_string();
                let links = article.body_links.clone();
                async move { wiki.article_buttons_for_links(&language, &links).await }
            })
        });

        let navigation = (!article.nav_templates.is_empty()).then(|| {
            spawn_wiki_task({
                let wiki = self.wiki.clone();
                let language = language.to_string();
                let nav_templates = article.nav_templates.clone();
                async move {
                    wiki.article_buttons_for_nav_templates(&language, &nav_templates)
                        .await
                }
            })
        });

        let disambiguation = (!article.disambiguation_groups.is_empty()).then(|| {
            spawn_wiki_task({
                let wiki = self.wiki.clone();
                let language = language.to_string();
                let groups = article.disambiguation_groups.clone();
                async move {
                    wiki.article_button_groups_for_link_groups(&language, &groups)
                        .await
                }
            })
        });

        let metadata = Some(spawn_wiki_task({
            let wiki = self.wiki.clone();
            let language = language.to_string();
            let revision_limit = self.config.metadata_revision_limit;
            async move {
                wiki.article_metadata(&language, page_id, revision_limit)
                    .await
            }
        }));

        let media = Some(spawn_wiki_task({
            let wiki = self.wiki.clone();
            let language = language.to_string();
            async move { wiki.article_media(&language, page_id).await }
        }));

        ArticleFollowupTasks {
            started: true,
            categories,
            body_links,
            navigation,
            disambiguation,
            metadata,
            media,
        }
    }

    /// Sends article references after the body and before link/navigation buttons.
    async fn send_article_references(&self, chat_id: i64, article: &ArticleContent) -> Result<()> {
        let Some(references_html) = article
            .references_html
            .as_deref()
            .map(str::trim)
            .filter(|references| !references.is_empty())
        else {
            return Ok(());
        };

        for part in render_references_html_parts(references_html, self.config.message_char_limit) {
            self.send_html_message(chat_id, &part, None).await?;
        }

        Ok(())
    }

    /// Sends the article follow-up messages in the user-facing order.
    async fn send_article_followups(
        &self,
        chat_id: i64,
        language: &str,
        page_id: u64,
        article: &ArticleContent,
        mut tasks: ArticleFollowupTasks,
        inline_images: &[MediaItem],
    ) -> Result<()> {
        self.send_article_body_links_followup(chat_id, language, tasks.body_links.take())
            .await?;
        self.send_article_disambiguation_followup(chat_id, language, tasks.disambiguation.take())
            .await?;
        self.send_article_navigation_followup(chat_id, language, tasks.navigation.take())
            .await?;
        self.send_article_categories_followup(chat_id, language, page_id, tasks.categories.take())
            .await?;
        self.send_article_metadata_followup(chat_id, tasks.metadata.take())
            .await?;
        self.send_article_media_followup(
            chat_id,
            language,
            page_id,
            article,
            tasks.media.take(),
            inline_images,
        )
        .await
    }

    /// Sends the article-body link buttons if their lookup succeeded.
    async fn send_article_body_links_followup(
        &self,
        chat_id: i64,
        language: &str,
        task: Option<JoinHandle<Result<Vec<ArticleButton>>>>,
    ) -> Result<()> {
        let Some(task) = task else {
            return Ok(());
        };

        match join_task(task).await {
            Ok(links) => {
                self.send_article_buttons_message(chat_id, language, "Article links", &links)
                    .await?;
            }
            Err(err) => {
                warn!(error = %format_error_chain(&err), "failed to fetch article body links")
            }
        }

        Ok(())
    }

    /// Sends grouped disambiguation target buttons.
    async fn send_article_disambiguation_followup(
        &self,
        chat_id: i64,
        language: &str,
        task: Option<JoinHandle<Result<Vec<ArticleButtonGroup>>>>,
    ) -> Result<()> {
        let Some(task) = task else {
            return Ok(());
        };

        match join_task(task).await {
            Ok(groups) => {
                for group in groups {
                    self.send_article_buttons_message(
                        chat_id,
                        language,
                        &format!("Disambiguation: {}", group.title),
                        &group.buttons,
                    )
                    .await?;
                }
            }
            Err(err) => {
                warn!(error = %format_error_chain(&err), "failed to fetch disambiguation links")
            }
        }

        Ok(())
    }

    /// Sends navigation-template buttons.
    async fn send_article_navigation_followup(
        &self,
        chat_id: i64,
        language: &str,
        task: Option<JoinHandle<Result<Vec<NavTemplateButtons>>>>,
    ) -> Result<()> {
        let Some(task) = task else {
            return Ok(());
        };

        match join_task(task).await {
            Ok(templates) => {
                for template in templates {
                    if template.buttons.is_empty() {
                        continue;
                    }
                    self.send_article_buttons_message_with_heading_html(
                        chat_id,
                        language,
                        &render_navigation_heading_html(&template),
                        &template.buttons,
                    )
                    .await?;
                }
            }
            Err(err) => {
                warn!(error = %format_error_chain(&err), "failed to fetch navigation template links")
            }
        }

        Ok(())
    }

    /// Sends article categories and their callback buttons.
    async fn send_article_categories_followup(
        &self,
        chat_id: i64,
        language: &str,
        page_id: u64,
        task: Option<JoinHandle<Result<Vec<Category>>>>,
    ) -> Result<()> {
        let Some(task) = task else {
            return Ok(());
        };

        match join_task(task).await {
            Ok(categories) => {
                if !categories.is_empty() {
                    let categories_html = render_categories_html(language, &categories);
                    self.send_html_message(
                        chat_id,
                        &categories_html,
                        article_categories_keyboard(language, page_id, &categories),
                    )
                    .await?;
                }
            }
            Err(err) => warn!(error = %err, "failed to fetch article categories"),
        }

        Ok(())
    }

    /// Sends article metadata with the Wikidata button on the first part.
    async fn send_article_metadata_followup(
        &self,
        chat_id: i64,
        task: Option<JoinHandle<Result<ArticleMetadata>>>,
    ) -> Result<()> {
        let Some(task) = task else {
            return Ok(());
        };

        match join_task(task).await {
            Ok(metadata) => {
                let metadata_html = render_metadata_html(&metadata);
                let metadata_parts =
                    split_html_lines_for_telegram(&metadata_html, self.config.message_char_limit);
                let wikidata_keyboard = wikidata_metadata_keyboard(&metadata);
                for (index, part) in metadata_parts.iter().enumerate() {
                    self.send_html_message(
                        chat_id,
                        part,
                        if index == 0 {
                            wikidata_keyboard.clone()
                        } else {
                            None
                        },
                    )
                    .await?;
                }
            }
            Err(err) => {
                warn!(error = %err, "failed to fetch article metadata");
                self.send_message(chat_id, "Metadata fetch failed.", None)
                    .await?;
            }
        }

        Ok(())
    }

    /// Sends article media while skipping images already emitted inline with
    /// body sections.
    async fn send_article_media_followup(
        &self,
        chat_id: i64,
        language: &str,
        page_id: u64,
        article: &ArticleContent,
        task: Option<JoinHandle<Result<ArticleMedia>>>,
        inline_images: &[MediaItem],
    ) -> Result<()> {
        let Some(task) = task else {
            return Ok(());
        };

        match join_task(task).await {
            Ok(media) => {
                self.send_media(
                    chat_id,
                    ArticleMediaSend {
                        language,
                        page_id,
                        title: &article.title,
                        media: &media,
                        infobox_image: article.infobox_image.as_ref(),
                        excluded_images: inline_images,
                    },
                )
                .await?
            }
            Err(err) => warn!(error = %err, "failed to fetch article media"),
        }

        Ok(())
    }

    /// Sends a simple heading with article callback buttons.
    async fn send_article_buttons_message(
        &self,
        chat_id: i64,
        language: &str,
        heading: &str,
        buttons: &[ArticleButton],
    ) -> Result<()> {
        self.send_article_buttons_message_with_heading_html(
            chat_id,
            language,
            &html_bold(heading),
            buttons,
        )
        .await
    }

    /// Sends article callback buttons under a pre-rendered Telegram HTML
    /// heading.
    async fn send_article_buttons_message_with_heading_html(
        &self,
        chat_id: i64,
        language: &str,
        heading_html: &str,
        buttons: &[ArticleButton],
    ) -> Result<()> {
        if buttons.is_empty() {
            return Ok(());
        }

        self.send_html_message(
            chat_id,
            heading_html,
            Some(json!({
                "inline_keyboard": article_results_keyboard(
                    language,
                    buttons,
                    self.config.big_article_char_threshold,
                    self.config.small_article_char_threshold,
                    false
                )
            })),
        )
        .await
    }

    /// Fetches and sends Wikidata claims and Wikidata page metadata.
    async fn send_wikidata(&self, chat_id: i64, language: &str, item: &str) -> Result<()> {
        self.send_chat_action(chat_id, "typing").await?;

        let claims = self.wiki.wikidata_claims(language, item);
        let page_metadata = self
            .wiki
            .wikidata_page_metadata(item, self.config.metadata_revision_limit);
        let (claims, page_metadata) = tokio::join!(claims, page_metadata);

        match claims {
            Ok(claims) => {
                for part in
                    render_wikidata_claims_html_parts(&claims, self.config.message_char_limit)
                {
                    self.send_html_message(chat_id, &part, None).await?;
                }
            }
            Err(err) => {
                warn!(error = %err, item, "failed to fetch Wikidata claims");
                self.send_message(chat_id, "Wikidata claims fetch failed.", None)
                    .await?;
            }
        }

        match page_metadata {
            Ok(metadata) => {
                let metadata_html = render_wikidata_page_metadata_html(&metadata);
                for part in
                    split_html_lines_for_telegram(&metadata_html, self.config.message_char_limit)
                {
                    self.send_html_message(chat_id, &part, None).await?;
                }
            }
            Err(err) => {
                warn!(error = %err, item, "failed to fetch Wikidata page metadata");
                self.send_message(chat_id, "Wikidata metadata fetch failed.", None)
                    .await?;
            }
        }

        Ok(())
    }

    /// Sends media follow-ups after the article body, including optional image
    /// galleries and audio.
    async fn send_media(&self, chat_id: i64, context: ArticleMediaSend<'_>) -> Result<()> {
        let images = filter_excluded_media_items(
            article_gallery_images(context.media, context.infobox_image),
            context.excluded_images,
        );
        if self.config.article_images_button_only {
            if let Some(keyboard) =
                article_images_keyboard(context.language, context.page_id, images.len())
            {
                self.send_html_message(chat_id, &html_bold("Article images"), Some(keyboard))
                    .await?;
            }
        } else if !images.is_empty() {
            self.send_image_gallery(chat_id, &images).await?;
        }

        if let Some(audio) = &context.media.audio {
            self.send_chat_action(chat_id, "upload_voice").await?;
            if let Err(err) = self
                .telegram_post(
                    "sendAudio",
                    &json!({
                        "chat_id": chat_id,
                        "audio": audio.url,
                        "title": truncate_for_telegram(&audio.title, 64),
                        "caption": truncate_for_telegram(
                            &format!("Audio from {}", context.title),
                            1024
                        )
                    }),
                )
                .await
            {
                warn!(error = %err, "failed to send article audio");
            }
        }

        Ok(())
    }

    /// Loads and sends the article image gallery for an Images callback.
    async fn send_article_images(&self, chat_id: i64, language: &str, page_id: u64) -> Result<()> {
        self.send_chat_action(chat_id, "upload_photo").await?;

        let article = self.wiki.article_content(language, page_id);
        let media = self.wiki.article_media(language, page_id);
        let (article, media) = tokio::join!(article, media);
        let article = article.with_context(|| {
            format!("failed to fetch article page_id={page_id} from {language}")
        })?;
        let media = media.with_context(|| {
            format!("failed to fetch article media page_id={page_id} from {language}")
        })?;
        let images = article_gallery_images(&media, article.infobox_image.as_ref());

        if images.is_empty() {
            self.send_message(chat_id, "No article images found.", None)
                .await?;
            return Ok(());
        }

        self.send_image_gallery(chat_id, &images).await
    }

    /// Sends images as a Telegram media group, falling back to individual sends
    /// when Telegram rejects the group.
    async fn send_image_gallery(&self, chat_id: i64, images: &[MediaItem]) -> Result<()> {
        let images = images
            .iter()
            .take(ARTICLE_IMAGE_GALLERY_LIMIT)
            .collect::<Vec<_>>();
        if images.is_empty() {
            return Ok(());
        }

        self.send_chat_action(chat_id, "upload_photo").await?;
        if images.len() == 1 {
            if !self.send_media_item(chat_id, images[0]).await? {
                self.send_message(
                    chat_id,
                    "Article image found, but Telegram could not fetch it.",
                    None,
                )
                .await?;
            }
            return Ok(());
        }

        let media = images
            .iter()
            .map(|image| {
                let mut item = json!({
                    "type": "photo",
                    "media": image.url,
                });
                if let Some(caption) = image.caption.as_ref() {
                    item["caption"] = Value::String(caption_html_for_telegram(caption));
                    item["parse_mode"] = Value::String("HTML".to_string());
                }
                item
            })
            .collect::<Vec<_>>();

        if let Err(err) = self
            .telegram_post(
                "sendMediaGroup",
                &json!({
                    "chat_id": chat_id,
                    "media": media,
                }),
            )
            .await
        {
            warn!(error = %err, "failed to send article image gallery");
            self.send_image_gallery_individually(chat_id, &images)
                .await?;
        }

        Ok(())
    }

    /// Sends images one by one when media groups are not usable for a page.
    async fn send_image_gallery_individually(
        &self,
        chat_id: i64,
        images: &[&MediaItem],
    ) -> Result<()> {
        let mut sent = 0usize;
        for image in images {
            if self.send_media_item(chat_id, image).await? {
                sent += 1;
            }
        }

        if sent == 0 {
            self.send_message(
                chat_id,
                "Article images found, but Telegram could not fetch them.",
                None,
            )
            .await?;
        }

        Ok(())
    }

    /// Sends one photo using the original URL and optional fallback thumbnail.
    async fn send_media_item(&self, chat_id: i64, image: &MediaItem) -> Result<bool> {
        self.send_chat_action(chat_id, "upload_photo").await?;
        let mut payload = json!({
            "chat_id": chat_id,
            "photo": image.url,
        });
        add_media_caption_payload(&mut payload, image);

        if let Err(original_err) = self.telegram_post("sendPhoto", &payload).await {
            let mut document_payload = json!({
                "chat_id": chat_id,
                "document": image.url,
            });
            add_media_caption_payload(&mut document_payload, image);
            if self
                .telegram_post("sendDocument", &document_payload)
                .await
                .is_ok()
            {
                warn!(
                    error = %original_err,
                    title = %image.title,
                    url = %image.url,
                    "sent original image as document after photo send failed"
                );
                return Ok(true);
            }

            if let Some(fallback_url) = image.fallback_url.as_ref() {
                payload["photo"] = Value::String(fallback_url.clone());
                if self.telegram_post("sendPhoto", &payload).await.is_ok() {
                    warn!(
                        error = %original_err,
                        title = %image.title,
                        url = %image.url,
                        fallback_url,
                        "sent fallback thumbnail after original image failed"
                    );
                    return Ok(true);
                }
            }

            warn!(
                error = %original_err,
                title = %image.title,
                url = %image.url,
                "failed to send article image"
            );
            return Ok(false);
        }

        Ok(true)
    }

    /// Sends a plain text Telegram message.
    async fn send_message(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        self.send_message_with_parse_mode(chat_id, text, reply_markup, None)
            .await
    }

    /// Sends a Telegram message with HTML parse mode.
    async fn send_html_message(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        self.send_message_with_parse_mode(chat_id, text, reply_markup, Some("HTML"))
            .await
    }

    /// Sends a Telegram message with optional parse mode and reply markup.
    async fn send_message_with_parse_mode(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<Value>,
        parse_mode: Option<&str>,
    ) -> Result<()> {
        let mut payload = json!({
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": false,
        });

        if let Some(parse_mode) = parse_mode {
            payload["parse_mode"] = Value::String(parse_mode.to_string());
        }

        if let Some(reply_markup) = reply_markup {
            payload["reply_markup"] = reply_markup;
        }

        self.telegram_post("sendMessage", &payload).await
    }

    /// Sends Telegram chat actions such as `typing` or `upload_photo`.
    async fn send_chat_action(&self, chat_id: i64, action: &str) -> Result<()> {
        self.telegram_post(
            "sendChatAction",
            &json!({
                "chat_id": chat_id,
                "action": action,
            }),
        )
        .await
    }

    /// Acknowledges a callback query to clear Telegram's loading spinner.
    async fn answer_callback_query(
        &self,
        callback_query_id: &str,
        text: &str,
        show_alert: bool,
    ) -> Result<()> {
        self.telegram_post(
            "answerCallbackQuery",
            &json!({
                "callback_query_id": callback_query_id,
                "text": text,
                "show_alert": show_alert,
            }),
        )
        .await
    }

    /// Answers an inline query with article results.
    async fn answer_inline_query(
        &self,
        inline_query_id: &str,
        results: Vec<Value>,
        cache_time: u32,
        is_personal: bool,
    ) -> Result<()> {
        self.telegram_post(
            "answerInlineQuery",
            &json!({
                "inline_query_id": inline_query_id,
                "results": results,
                "cache_time": cache_time,
                "is_personal": is_personal,
            }),
        )
        .await
    }

    /// Posts a JSON payload to one Telegram Bot API method.
    async fn telegram_post(&self, method: &str, payload: &Value) -> Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/{}",
            self.config.telegram_bot_token, method
        );
        let response = self
            .http
            .post(url)
            .json(payload)
            .send()
            .await
            .with_context(|| format!("failed to call Telegram method {method}"))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("Telegram method {method} failed with {status}: {body}");
        }

        Ok(())
    }
}

#[derive(Default)]
struct ArticleFollowupTasks {
    started: bool,
    categories: Option<JoinHandle<Result<Vec<Category>>>>,
    body_links: Option<JoinHandle<Result<Vec<ArticleButton>>>>,
    navigation: Option<JoinHandle<Result<Vec<NavTemplateButtons>>>>,
    disambiguation: Option<JoinHandle<Result<Vec<ArticleButtonGroup>>>>,
    metadata: Option<JoinHandle<Result<ArticleMetadata>>>,
    media: Option<JoinHandle<Result<ArticleMedia>>>,
}

#[derive(Clone)]
struct WikiClient {
    http: Client,
    cache_max_entries: usize,
}

impl WikiClient {
    /// Creates a Wikimedia API client with a shared HTTP client and RAM cache
    /// capacity.
    fn new(http: Client, cache_max_entries: usize) -> Self {
        Self {
            http,
            cache_max_entries,
        }
    }

    /// Searches article pages for normal chat mode.
    async fn search(
        &self,
        language: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<ArticleButton>> {
        let cache_key = format!("search:{language}:{limit}:{query}");
        if let Some(results) = self.cache_get(&cache_key, |cache| &mut cache.search) {
            return Ok(results);
        }

        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("generator", "search".to_string()),
            ("gsrsearch", query.to_string()),
            ("gsrnamespace", "0".to_string()),
            ("gsrlimit", limit.min(MAX_SEARCH_LIMIT).to_string()),
            ("prop", "info|pageprops".to_string()),
            ("utf8", "1".to_string()),
        ];

        let response: CategoryInfoResponse = self
            .http
            .get(wiki_api_url(language)?)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut pages = response.query.map(|query| query.pages).unwrap_or_default();
        pages.sort_by_key(|page| page.index.unwrap_or(usize::MAX));

        let results = pages
            .into_iter()
            .filter_map(|page| {
                Some(ArticleButton {
                    page_id: page.pageid?,
                    title: page.title.clone(),
                    size: page.length,
                    wordcount: None,
                    snippet: None,
                    is_disambiguation: info_page_is_disambiguation(&page),
                })
            })
            .collect::<Vec<_>>();
        self.cache_put(cache_key, &results, |cache| &mut cache.search);
        Ok(results)
    }

    /// Searches article pages for inline mode and fetches lead extracts in the
    /// same request.
    async fn inline_search(
        &self,
        language: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<ArticleButton>> {
        let cache_key = format!("inline_search:{language}:{limit}:{query}");
        if let Some(results) = self.cache_get(&cache_key, |cache| &mut cache.search) {
            return Ok(results);
        }

        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("generator", "search".to_string()),
            ("gsrsearch", query.to_string()),
            ("gsrnamespace", "0".to_string()),
            ("gsrlimit", limit.min(MAX_SEARCH_LIMIT).to_string()),
            ("prop", "extracts|info|pageprops".to_string()),
            ("exintro", "1".to_string()),
            ("explaintext", "1".to_string()),
            ("inprop", "url".to_string()),
        ];

        let response: CategoryInfoResponse = self
            .http
            .get(wiki_api_url(language)?)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut pages = response.query.map(|query| query.pages).unwrap_or_default();
        pages.sort_by_key(|page| page.index.unwrap_or(usize::MAX));

        let results = pages
            .into_iter()
            .filter_map(|page| {
                let is_disambiguation = info_page_is_disambiguation(&page);
                Some(ArticleButton {
                    page_id: page.pageid?,
                    title: page.title,
                    size: page.length,
                    wordcount: None,
                    snippet: page
                        .extract
                        .map(|extract| normalize_inline_extract(&extract))
                        .filter(|snippet| !snippet.is_empty()),
                    is_disambiguation,
                })
            })
            .collect::<Vec<_>>();
        self.cache_put(cache_key, &results, |cache| &mut cache.search);
        Ok(results)
    }

    /// Searches category pages by inclusion-style title matching.
    async fn search_categories(
        &self,
        language: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Category>> {
        let query = query.trim();
        let cache_key = format!("category_search:{language}:{limit}:{query}");
        if let Some(categories) = self.cache_get(&cache_key, |cache| &mut cache.category_search) {
            return Ok(categories);
        }

        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("list", "search".to_string()),
            ("srnamespace", "14".to_string()),
            ("srsearch", category_search_query(query)),
            ("srlimit", CATEGORY_SEARCH_FETCH_LIMIT.to_string()),
            ("srprop", "snippet|wordcount|size|timestamp".to_string()),
            ("utf8", "1".to_string()),
        ];

        let response: SearchResponse = self
            .http
            .get(wiki_api_url(language)?)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut ranked = response
            .query
            .search
            .into_iter()
            .enumerate()
            .map(|(rank, hit)| {
                (
                    rank,
                    Category {
                        title: normalize_category_title_for_language(&hit.title, language),
                        article_count: None,
                    },
                )
            })
            .filter(|(_, category)| category_title_matches_query(&category.title, query))
            .collect::<Vec<_>>();

        ranked.sort_by_key(|(rank, category)| category_search_score(&category.title, query, *rank));
        let categories = ranked
            .into_iter()
            .map(|(_, category)| category)
            .take(limit.min(MAX_SEARCH_LIMIT))
            .collect::<Vec<_>>();

        self.cache_put(cache_key, &categories, |cache| &mut cache.category_search);
        Ok(categories)
    }

    /// Returns the first page of newest article members for a category.
    async fn category_articles(
        &self,
        language: &str,
        title: &str,
        limit: usize,
    ) -> Result<Vec<ArticleButton>> {
        Ok(self
            .category_articles_page(language, title, limit, 0)
            .await?
            .articles)
    }

    /// Returns one page of newest article members, resolving page IDs and sizes
    /// with a batched title lookup.
    async fn category_articles_page(
        &self,
        language: &str,
        title: &str,
        limit: usize,
        page_index: usize,
    ) -> Result<CategoryArticlePage> {
        let normalized_title = normalize_category_title_for_language(title, language);
        let limit = limit.clamp(1, MAX_SEARCH_LIMIT);
        let cache_key =
            format!("category_articles:{language}:{limit}:{page_index}:{normalized_title}");
        if let Some(page) = self.cache_get(&cache_key, |cache| &mut cache.category_articles) {
            return Ok(page);
        }

        let members = self
            .category_member_titles_page(language, &normalized_title, 0, limit, page_index)
            .await?;
        let titles = members
            .members
            .iter()
            .map(|member| member.title.clone())
            .collect::<Vec<_>>();
        let pages_by_title = self.article_pages_by_titles(language, &titles).await?;
        let articles = members
            .members
            .into_iter()
            .filter_map(|member| {
                let page = pages_by_title.get(&normalized_title_key(&member.title))?;
                Some(ArticleButton {
                    page_id: page.pageid?,
                    title: member.title,
                    size: page.length,
                    wordcount: None,
                    snippet: None,
                    is_disambiguation: info_page_is_disambiguation(page),
                })
            })
            .collect::<Vec<_>>();
        let page = CategoryArticlePage {
            articles,
            page_index,
            has_next: members.has_next,
        };
        self.cache_put(cache_key, &page, |cache| &mut cache.category_articles);
        Ok(page)
    }

    /// Returns one page of subcategory members for a category.
    async fn category_subcategories_page(
        &self,
        language: &str,
        title: &str,
        limit: usize,
        page_index: usize,
    ) -> Result<CategorySubcategoryPage> {
        let normalized_title = normalize_category_title_for_language(title, language);
        let limit = limit.clamp(1, MAX_SEARCH_LIMIT);
        let cache_key =
            format!("category_subcategories:{language}:{limit}:{page_index}:{normalized_title}");
        if let Some(page) = self.cache_get(&cache_key, |cache| &mut cache.category_subcategories) {
            return Ok(page);
        }

        let members = self
            .category_member_titles_page(language, &normalized_title, 14, limit, page_index)
            .await?;
        let categories = members
            .members
            .into_iter()
            .map(|member| Category {
                title: normalize_category_title_for_language(&member.title, language),
                article_count: None,
            })
            .collect::<Vec<_>>();
        let page = CategorySubcategoryPage {
            categories,
            page_index,
            has_next: members.has_next,
        };
        self.cache_put(cache_key, &page, |cache| &mut cache.category_subcategories);
        Ok(page)
    }

    /// Fetches raw category member titles for a namespace and page offset.
    async fn category_member_titles_page(
        &self,
        language: &str,
        normalized_title: &str,
        namespace: u8,
        limit: usize,
        page_index: usize,
    ) -> Result<CategoryMemberTitlePage> {
        let mut continuation: Option<String> = None;

        for current_page in 0..=page_index {
            let request_limit = if current_page == page_index {
                limit + 1
            } else {
                limit
            };
            let mut params = vec![
                ("action", "query".to_string()),
                ("format", "json".to_string()),
                ("formatversion", "2".to_string()),
                ("list", "categorymembers".to_string()),
                ("cmtitle", normalized_title.to_string()),
                ("cmnamespace", namespace.to_string()),
                ("cmsort", "timestamp".to_string()),
                ("cmdir", "desc".to_string()),
                ("cmprop", "ids|title|timestamp".to_string()),
                ("cmlimit", request_limit.to_string()),
            ];
            if let Some(value) = continuation.as_deref() {
                params.push(("cmcontinue", value.to_string()));
            }

            let response: CategoryMembersResponse = self
                .http
                .get(wiki_api_url(language)?)
                .query(&params)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;

            let mut members = response
                .query
                .map(|query| query.categorymembers)
                .unwrap_or_default();
            let next_continuation = response
                .continuation
                .and_then(|continuation| continuation.cmcontinue);

            if current_page == page_index {
                let has_next = members.len() > limit || next_continuation.is_some();
                members.truncate(limit);
                return Ok(CategoryMemberTitlePage { members, has_next });
            }

            let Some(next) = next_continuation else {
                return Ok(CategoryMemberTitlePage {
                    members: Vec::new(),
                    has_next: false,
                });
            };
            continuation = Some(next);
        }

        Ok(CategoryMemberTitlePage {
            members: Vec::new(),
            has_next: false,
        })
    }

    /// Resolves a category page ID back to its localized title.
    async fn category_title_by_page_id(&self, language: &str, page_id: u64) -> Result<String> {
        let cache_key = format!("category_title:{language}:{page_id}");
        if let Some(title) = self.cache_get(&cache_key, |cache| &mut cache.category_titles) {
            return Ok(title);
        }

        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("prop", "info|pageprops".to_string()),
            ("pageids", page_id.to_string()),
            ("redirects", "1".to_string()),
        ];

        let response: InfoResponse = self
            .http
            .get(wiki_api_url(language)?)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let title = response
            .query
            .pages
            .into_iter()
            .next()
            .map(|page| normalize_category_title_for_language(&page.title, language))
            .context("Wikipedia returned no category title")?;
        self.cache_put(cache_key, &title, |cache| &mut cache.category_titles);
        Ok(title)
    }

    /// Fetches category description, parent categories, and member counts.
    async fn category_overview(&self, language: &str, title: &str) -> Result<CategoryOverview> {
        let normalized_title = normalize_category_title_for_language(title, language);
        let cache_key = format!("category_overview:{language}:{normalized_title}");
        if let Some(overview) = self.cache_get(&cache_key, |cache| &mut cache.category_overviews) {
            return Ok(overview);
        }

        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("prop", "extracts|categories|categoryinfo".to_string()),
            ("titles", normalized_title),
            ("exintro", "1".to_string()),
            ("explaintext", "1".to_string()),
            ("cllimit", "max".to_string()),
            ("clshow", "!hidden".to_string()),
            ("redirects", "1".to_string()),
        ];

        let response: InfoResponse = self
            .http
            .get(wiki_api_url(language)?)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let page = response
            .query
            .pages
            .into_iter()
            .next()
            .context("Wikipedia returned no category page")?;
        let description = page
            .extract
            .map(|extract| extract.trim().to_string())
            .filter(|extract| !extract.is_empty());
        let article_count = page.categoryinfo.as_ref().map(|info| info.pages);
        let subcategory_count = page.categoryinfo.as_ref().map(|info| info.subcats);
        let parent_categories = page
            .categories
            .unwrap_or_default()
            .into_iter()
            .map(|category| Category {
                title: normalize_category_title_for_language(&category.title, language),
                article_count: None,
            })
            .collect::<Vec<_>>();
        let overview = CategoryOverview {
            page_id: page.pageid,
            article_count,
            subcategory_count,
            description,
            parent_categories,
        };
        self.cache_put(cache_key, &overview, |cache| &mut cache.category_overviews);
        Ok(overview)
    }

    /// Loads an article from cache or fetches and parses its MediaWiki HTML.
    async fn article_content(&self, language: &str, page_id: u64) -> Result<ArticleContent> {
        let cache_key = format!("article_content:{language}:{page_id}");
        if let Some(article) = self.cache_get(&cache_key, |cache| &mut cache.article_content) {
            return Ok(article);
        }

        let article = self.article_parsed_html(language, page_id).await?;
        self.cache_put(cache_key, &article, |cache| &mut cache.article_content);
        Ok(article)
    }

    /// Fetches parsed article HTML and extracts the bot's renderable article
    /// model from one Wikimedia parse call.
    async fn article_parsed_html(&self, language: &str, page_id: u64) -> Result<ArticleContent> {
        let params = vec![
            ("action", "parse".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("pageid", page_id.to_string()),
            ("prop", "text|properties".to_string()),
            ("disableeditsection", "1".to_string()),
            ("disabletoc", "1".to_string()),
            ("redirects", "1".to_string()),
        ];

        let response = self.article_parse_response(language, &params).await?;

        let parsed = response
            .parse
            .context("Wikipedia parse response did not include article data")?;
        let html = parsed
            .text
            .clone()
            .context("Wikipedia parse response did not include HTML")?;
        let title = parsed
            .title
            .clone()
            .unwrap_or_else(|| format!("Page {page_id}"));
        let body_sections = mediawiki_html_to_telegram_sections(language, &title, &html);
        let body_html = article_body_sections_to_html(language, &title, &body_sections);
        let extract = body_html
            .as_deref()
            .map(html_to_plain_text)
            .unwrap_or_else(|| html_to_text(&html));
        let length = (!extract.is_empty()).then_some(extract.chars().count());
        let is_disambiguation = is_disambiguation_page(&parsed, &html);
        let disambiguation_groups = if is_disambiguation {
            mediawiki_disambiguation_link_groups(language, &html)
        } else {
            Vec::new()
        };
        let body_links = if is_disambiguation {
            Vec::new()
        } else {
            mediawiki_body_links(language, &title, &html)
        };

        Ok(ArticleContent {
            title,
            extract,
            length,
            infobox: extract_infobox_text(language, &html),
            infobox_image: extract_infobox_image(&html),
            body_html,
            body_sections,
            references_html: mediawiki_references_to_telegram_html(language, &html),
            body_links,
            nav_templates: mediawiki_nav_templates(language, &html),
            is_disambiguation,
            disambiguation_groups,
        })
    }

    /// Executes the article parse request with one retry for transient failures.
    async fn article_parse_response(
        &self,
        language: &str,
        params: &[(&str, String)],
    ) -> Result<ParseResponse> {
        let url = wiki_api_url(language)?;
        let mut last_error = None;
        for attempt in 1..=2 {
            let result: Result<ParseResponse> = async {
                let response = self
                    .http
                    .get(url.clone())
                    .query(params)
                    .send()
                    .await
                    .context("Wikipedia parse request failed")?
                    .error_for_status()
                    .context("Wikipedia parse request returned an error status")?;
                response
                    .json()
                    .await
                    .context("failed to decode Wikipedia parse response")
            }
            .await;

            match result {
                Ok(response) => return Ok(response),
                Err(err) if attempt == 1 => {
                    warn!(error = %format_error_chain(&err), "retrying Wikipedia article parse request");
                    last_error = Some(err);
                }
                Err(err) => return Err(err),
            }
        }

        Err(last_error.expect("article parse retry loop stores first error"))
    }

    /// Converts parsed internal links into article buttons with one batched
    /// title lookup.
    async fn article_buttons_for_links(
        &self,
        language: &str,
        links: &[NavLink],
    ) -> Result<Vec<ArticleButton>> {
        if links.is_empty() {
            return Ok(Vec::new());
        }

        let titles = links
            .iter()
            .take(ARTICLE_TITLE_LOOKUP_LIMIT)
            .map(|link| link.page_title.clone())
            .collect::<Vec<_>>();
        let pages_by_title = self.article_pages_by_titles(language, &titles).await?;

        Ok(links
            .iter()
            .take(ARTICLE_TITLE_LOOKUP_LIMIT)
            .filter_map(|link| {
                let page = pages_by_title.get(&normalized_title_key(&link.page_title))?;
                Some(ArticleButton {
                    page_id: page.pageid?,
                    title: link.label.clone(),
                    size: page.length,
                    wordcount: None,
                    snippet: None,
                    is_disambiguation: info_page_is_disambiguation(page),
                })
            })
            .collect())
    }

    /// Converts grouped disambiguation links into grouped buttons while keeping
    /// the underlying title lookup batched.
    async fn article_button_groups_for_link_groups(
        &self,
        language: &str,
        groups: &[DisambiguationLinkGroup],
    ) -> Result<Vec<ArticleButtonGroup>> {
        let titles = groups
            .iter()
            .flat_map(|group| group.links.iter())
            .take(ARTICLE_TITLE_LOOKUP_LIMIT)
            .map(|link| link.page_title.clone())
            .collect::<Vec<_>>();
        let pages_by_title = self.article_pages_by_titles(language, &titles).await?;

        Ok(groups
            .iter()
            .filter_map(|group| {
                let buttons = group
                    .links
                    .iter()
                    .filter_map(|link| {
                        let page = pages_by_title.get(&normalized_title_key(&link.page_title))?;
                        Some(ArticleButton {
                            page_id: page.pageid?,
                            title: link.label.clone(),
                            size: page.length,
                            wordcount: None,
                            snippet: None,
                            is_disambiguation: info_page_is_disambiguation(page),
                        })
                    })
                    .collect::<Vec<_>>();
                (!buttons.is_empty()).then(|| ArticleButtonGroup {
                    title: group.title.clone(),
                    buttons,
                })
            })
            .collect())
    }

    /// Converts navigation template links into grouped article buttons with one
    /// batched title lookup.
    async fn article_buttons_for_nav_templates(
        &self,
        language: &str,
        nav_templates: &[NavTemplate],
    ) -> Result<Vec<NavTemplateButtons>> {
        let links = nav_templates
            .iter()
            .flat_map(|template| template.links.iter())
            .collect::<Vec<_>>();
        if links.is_empty() {
            return Ok(Vec::new());
        }

        let titles = links
            .iter()
            .take(ARTICLE_TITLE_LOOKUP_LIMIT)
            .map(|link| link.page_title.clone())
            .collect::<Vec<_>>();
        let pages_by_title = self.article_pages_by_titles(language, &titles).await?;

        Ok(nav_templates
            .iter()
            .filter_map(|template| {
                let buttons = template
                    .links
                    .iter()
                    .filter_map(|link| {
                        let page = pages_by_title.get(&normalized_title_key(&link.page_title))?;
                        Some(ArticleButton {
                            page_id: page.pageid?,
                            title: link.label.clone(),
                            size: page.length,
                            wordcount: None,
                            snippet: None,
                            is_disambiguation: info_page_is_disambiguation(page),
                        })
                    })
                    .collect::<Vec<_>>();
                (!buttons.is_empty()).then(|| NavTemplateButtons {
                    title: template.title.clone(),
                    title_url: template.title_url.clone(),
                    buttons,
                })
            })
            .collect())
    }

    /// Fetches MediaWiki info for many titles in one query.
    async fn article_pages_by_titles(
        &self,
        language: &str,
        titles: &[String],
    ) -> Result<HashMap<String, InfoPage>> {
        let mut unique_titles = Vec::new();
        let mut seen = HashSet::new();
        for title in titles.iter().take(ARTICLE_TITLE_LOOKUP_LIMIT) {
            if seen.insert(normalized_title_key(title)) {
                unique_titles.push(title.as_str());
            }
        }
        if unique_titles.is_empty() {
            return Ok(HashMap::new());
        }

        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("prop", "info".to_string()),
            ("titles", unique_titles.join("|")),
        ];

        let response: InfoResponse = self
            .http
            .get(wiki_api_url(language)?)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(response
            .query
            .pages
            .into_iter()
            .map(|page| (normalized_title_key(&page.title), page))
            .collect())
    }

    /// Fetches article categories and their article counts.
    async fn article_categories(&self, language: &str, page_id: u64) -> Result<Vec<Category>> {
        let cache_key = format!("article_categories:{language}:{page_id}");
        if let Some(categories) = self.cache_get(&cache_key, |cache| &mut cache.article_categories)
        {
            return Ok(categories);
        }

        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("generator", "categories".to_string()),
            ("pageids", page_id.to_string()),
            ("gcllimit", "max".to_string()),
            ("gclshow", "!hidden".to_string()),
            ("prop", "categoryinfo".to_string()),
            ("redirects", "1".to_string()),
        ];

        let response: CategoryInfoResponse = self
            .http
            .get(wiki_api_url(language)?)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut categories = response
            .query
            .map(|query| query.pages)
            .unwrap_or_default()
            .into_iter()
            .map(|page| Category {
                title: normalize_category_title_for_language(&page.title, language),
                article_count: page.categoryinfo.map(|info| info.pages),
            })
            .collect::<Vec<_>>();
        categories.sort_by_key(|category| normalized_title_key(&category.title));
        self.cache_put(cache_key, &categories, |cache| {
            &mut cache.article_categories
        });
        Ok(categories)
    }

    /// Fetches article metadata that is intentionally sent after the body.
    async fn article_metadata(
        &self,
        language: &str,
        page_id: u64,
        revision_limit: usize,
    ) -> Result<ArticleMetadata> {
        let cache_key = format!("article_metadata:{language}:{page_id}:{revision_limit}");
        if let Some(metadata) = self.cache_get(&cache_key, |cache| &mut cache.article_metadata) {
            return Ok(metadata);
        }

        let parts = self
            .article_metadata_parts(language, page_id, revision_limit)
            .await?;
        let wikidata_entity = if let Some(wikidata_item) =
            parts.info.page_props.get("wikibase_item")
        {
            self.wikidata_entity(wikidata_item)
                .await
                .map(Some)
                .unwrap_or_else(|err| {
                    warn!(error = %err, wikidata_item, "failed to fetch Wikidata entity for article metadata");
                    None
                })
        } else {
            None
        };
        let commons_url = match commons_page_url(&parts.info.page_props) {
            Some(url) => Some(url),
            None => wikidata_entity
                .as_ref()
                .and_then(wikidata_commons_url_from_entity),
        };
        let wikidata_property_count = wikidata_entity
            .as_ref()
            .and_then(|entity| entity.claims.as_ref())
            .map(HashMap::len);
        let metadata = ArticleMetadata {
            language: language.to_string(),
            info: parts.info,
            commons_url,
            wikidata_property_count,
            revisions: parts.revisions,
            external_links: parts.external_links,
            language_links: parts.language_links,
            pageviews: parts.pageviews,
        };
        self.cache_put(cache_key, &metadata, |cache| &mut cache.article_metadata);
        Ok(metadata)
    }

    /// Fetches a raw Wikidata entity document.
    async fn wikidata_entity(&self, wikidata_item: &str) -> Result<WikidataEntity> {
        let wikidata_item =
            normalize_wikidata_entity_id(wikidata_item).context("Wikidata item id is invalid")?;
        let cache_key = format!("wikidata_entity:{wikidata_item}");
        if let Some(entity) = self.cache_get(&cache_key, |cache| &mut cache.wikidata_entities) {
            return Ok(entity);
        }

        let params = vec![
            ("action", "wbgetentities".to_string()),
            ("format", "json".to_string()),
            ("ids", wikidata_item.clone()),
            ("props", "sitelinks|claims".to_string()),
            ("sitefilter", "commonswiki".to_string()),
        ];

        let response: WikidataEntitiesResponse = self
            .http
            .get(WIKIDATA_API_URL)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let entity = response
            .entities
            .get(&wikidata_item)
            .cloned()
            .context("Wikidata response did not include requested entity")?;
        self.cache_put(cache_key, &entity, |cache| &mut cache.wikidata_entities);
        Ok(entity)
    }

    /// Fetches the MediaWiki metadata parts that can be collected in parallel
    /// with Wikidata-specific lookups.
    async fn article_metadata_parts(
        &self,
        language: &str,
        page_id: u64,
        revision_limit: usize,
    ) -> Result<ArticleMetadataParts> {
        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            (
                "prop",
                "info|pageprops|coordinates|revisions|extlinks|langlinks|pageviews".to_string(),
            ),
            ("pageids", page_id.to_string()),
            ("inprop", "url".to_string()),
            ("coprop", "type|name|dim|country|region|globe".to_string()),
            ("rvlimit", revision_limit.min(50).to_string()),
            ("rvprop", "ids|timestamp|user|comment|size|tags".to_string()),
            ("ellimit", "max".to_string()),
            ("lllimit", "max".to_string()),
            ("llprop", "url|langname".to_string()),
            ("pvipdays", "30".to_string()),
            ("redirects", "1".to_string()),
        ];

        let response: InfoResponse = self
            .http
            .get(wiki_api_url(language)?)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let page = response
            .query
            .pages
            .into_iter()
            .next()
            .context("Wikipedia returned no page info")?;

        let pageviews = pageviews_from_map(page.pageviews);
        let info = ArticleInfo {
            page_id: page.pageid,
            title: page.title,
            full_url: page.fullurl,
            length: page.length,
            touched: page.touched,
            last_revision_id: page.lastrevid,
            page_props: page.pageprops.unwrap_or_default(),
            coordinates: page.coordinates.unwrap_or_default(),
        };
        let external_links = page
            .extlinks
            .unwrap_or_default()
            .into_iter()
            .filter_map(|link| link.url)
            .collect::<Vec<_>>();

        Ok(ArticleMetadataParts {
            info,
            revisions: page.revisions.unwrap_or_default(),
            external_links,
            language_links: page.langlinks.unwrap_or_default(),
            pageviews,
        })
    }

    /// Fetches and formats Wikidata claims for a Wikidata button callback.
    async fn wikidata_claims(&self, language: &str, item: &str) -> Result<WikidataClaims> {
        let language =
            normalize_language_code(language).context("Wikidata claims language is invalid")?;
        let item = normalize_wikidata_entity_id(item).context("Wikidata item id is invalid")?;
        let cache_key = format!("wikidata_claims:{language}:{item}");
        if let Some(claims) = self.cache_get(&cache_key, |cache| &mut cache.wikidata_claims) {
            return Ok(claims);
        }

        let entity = self.wikidata_entity(&item).await?;
        let claims_by_property = entity.claims.clone().unwrap_or_default();
        let mut label_ids = HashSet::from([item.clone()]);
        for (property_id, claims) in &claims_by_property {
            label_ids.insert(property_id.clone());
            for claim in claims {
                if let Some(entity_id) = wikidata_entity_id_from_snak(&claim.mainsnak)
                    .and_then(|id| normalize_wikidata_entity_id(&id))
                {
                    label_ids.insert(entity_id);
                }
                if let Some(unit_id) = claim
                    .mainsnak
                    .datavalue
                    .as_ref()
                    .and_then(|datavalue| wikidata_quantity_unit_id(&datavalue.value))
                {
                    label_ids.insert(unit_id);
                }
            }
        }

        let label_ids = label_ids.into_iter().collect::<Vec<_>>();
        let external_id_property_ids = claims_by_property
            .iter()
            .filter(|(_property_id, claims)| {
                claims.iter().any(|claim| {
                    claim.mainsnak.datatype.as_deref() == Some("external-id")
                        && claim
                            .mainsnak
                            .datavalue
                            .as_ref()
                            .and_then(|datavalue| datavalue.value.as_str())
                            .is_some()
                })
            })
            .map(|(property_id, _claims)| property_id.clone())
            .collect::<Vec<_>>();

        let labels = self.wikidata_labels(&language, &label_ids);
        let property_formatters = self.wikidata_property_formatters(&external_id_property_ids);
        let (labels, property_formatters) = tokio::join!(labels, property_formatters);
        let labels = labels.unwrap_or_else(|err| {
            warn!(error = %err, item, "failed to fetch Wikidata labels");
            HashMap::new()
        });
        let property_formatters = property_formatters.unwrap_or_else(|err| {
            warn!(error = %err, item, "failed to fetch Wikidata formatter URLs");
            HashMap::new()
        });

        let mut properties = claims_by_property
            .into_iter()
            .map(|(property_id, claims)| {
                let property_label = wikidata_label_text(&labels, &property_id)
                    .unwrap_or_else(|| property_id.clone());
                let values = claims
                    .iter()
                    .filter_map(|claim| {
                        render_wikidata_snak_value(
                            &property_id,
                            &claim.mainsnak,
                            &labels,
                            &property_formatters,
                        )
                    })
                    .take(WIKIDATA_VALUES_PER_PROPERTY_LIMIT)
                    .collect::<Vec<_>>();
                WikidataPropertyClaims {
                    property_id,
                    property_label,
                    total_values: claims.len(),
                    values,
                }
            })
            .collect::<Vec<_>>();
        properties.sort_by_key(|property| property.property_label.to_lowercase());

        let claims = WikidataClaims {
            item: item.clone(),
            label: wikidata_label_text(&labels, &item)
                .or_else(|| wikidata_localized_entity_value(entity.labels.as_ref(), &language)),
            description: wikidata_description_text(&labels, &item).or_else(|| {
                wikidata_localized_entity_value(entity.descriptions.as_ref(), &language)
            }),
            property_count: properties.len(),
            properties,
        };

        self.cache_put(cache_key, &claims, |cache| &mut cache.wikidata_claims);
        Ok(claims)
    }

    /// Loads formatter URL templates for Wikidata property IDs in batches.
    async fn wikidata_property_formatters(
        &self,
        property_ids: &[String],
    ) -> Result<HashMap<String, String>> {
        let mut unique_ids = property_ids
            .iter()
            .filter_map(|id| normalize_wikidata_entity_id(id))
            .filter(|id| id.starts_with('P'))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        unique_ids.sort();
        if unique_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut formatters = HashMap::new();
        let mut missing_ids = Vec::new();
        for property_id in unique_ids {
            let cache_key = format!("wikidata_property_formatter:{property_id}");
            match self.cache_get(&cache_key, |cache| &mut cache.wikidata_property_formatters) {
                Some(Some(formatter)) => {
                    formatters.insert(property_id, formatter);
                }
                Some(None) => {}
                None => missing_ids.push(property_id),
            }
        }

        for chunk in missing_ids.chunks(WIKIDATA_ENTITY_BATCH_SIZE) {
            let params = vec![
                ("action", "wbgetentities".to_string()),
                ("format", "json".to_string()),
                ("ids", chunk.join("|")),
                ("props", "claims".to_string()),
            ];

            let response: WikidataEntitiesResponse = self
                .http
                .get(WIKIDATA_API_URL)
                .query(&params)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;

            for property_id in chunk {
                let formatter = response
                    .entities
                    .get(property_id)
                    .and_then(wikidata_formatter_url_from_entity);
                let cache_key = format!("wikidata_property_formatter:{property_id}");
                self.cache_put(cache_key, &formatter, |cache| {
                    &mut cache.wikidata_property_formatters
                });
                if let Some(formatter) = formatter {
                    formatters.insert(property_id.clone(), formatter);
                }
            }
        }

        Ok(formatters)
    }

    /// Loads localized labels and descriptions for Wikidata entity IDs.
    async fn wikidata_labels(
        &self,
        language: &str,
        ids: &[String],
    ) -> Result<HashMap<String, WikidataLabelInfo>> {
        let mut unique_ids = ids
            .iter()
            .filter_map(|id| normalize_wikidata_entity_id(id))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        unique_ids.sort();
        if unique_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let cache_key = format!("wikidata_labels:{language}:{}", unique_ids.join("|"));
        if let Some(labels) = self.cache_get(&cache_key, |cache| &mut cache.wikidata_labels) {
            return Ok(labels);
        }

        let mut labels = HashMap::new();
        for chunk in unique_ids.chunks(WIKIDATA_ENTITY_BATCH_SIZE) {
            let params = vec![
                ("action", "wbgetentities".to_string()),
                ("format", "json".to_string()),
                ("ids", chunk.join("|")),
                ("props", "labels|descriptions".to_string()),
                ("languages", format!("{language}|en")),
                ("languagefallback", "1".to_string()),
            ];

            let response: WikidataEntitiesResponse = self
                .http
                .get(WIKIDATA_API_URL)
                .query(&params)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;

            for (id, entity) in response.entities {
                labels.insert(
                    id,
                    WikidataLabelInfo {
                        label: wikidata_localized_entity_value(entity.labels.as_ref(), language),
                        description: wikidata_localized_entity_value(
                            entity.descriptions.as_ref(),
                            language,
                        ),
                    },
                );
            }
        }

        self.cache_put(cache_key, &labels, |cache| &mut cache.wikidata_labels);
        Ok(labels)
    }

    /// Fetches MediaWiki metadata for a Wikidata entity page.
    async fn wikidata_page_metadata(
        &self,
        item: &str,
        revision_limit: usize,
    ) -> Result<WikidataPageMetadata> {
        let item = normalize_wikidata_entity_id(item).context("Wikidata item id is invalid")?;
        let cache_key = format!("wikidata_page_metadata:{item}:{revision_limit}");
        if let Some(metadata) =
            self.cache_get(&cache_key, |cache| &mut cache.wikidata_page_metadata)
        {
            return Ok(metadata);
        }

        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("prop", "info|revisions".to_string()),
            ("titles", item.clone()),
            ("inprop", "url".to_string()),
            ("rvlimit", revision_limit.min(50).to_string()),
            ("rvprop", "ids|timestamp|user|comment|size|tags".to_string()),
            ("redirects", "1".to_string()),
        ];

        let response: InfoResponse = self
            .http
            .get(WIKIDATA_API_URL)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let page = response
            .query
            .pages
            .into_iter()
            .next()
            .context("Wikidata returned no page info")?;
        let metadata = WikidataPageMetadata {
            item: item.clone(),
            page_id: page.pageid,
            title: page.title,
            full_url: page.fullurl,
            length: page.length,
            touched: page.touched,
            last_revision_id: page.lastrevid,
            revisions: page.revisions.unwrap_or_default(),
        };

        self.cache_put(cache_key, &metadata, |cache| {
            &mut cache.wikidata_page_metadata
        });
        Ok(metadata)
    }

    /// Fetches article image/audio metadata from MediaWiki.
    async fn article_media(&self, language: &str, page_id: u64) -> Result<ArticleMedia> {
        let cache_key = format!("article_media:{language}:{page_id}");
        if let Some(media) = self.cache_get(&cache_key, |cache| &mut cache.article_media) {
            return Ok(media);
        }

        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("prop", "pageimages|images".to_string()),
            ("pageids", page_id.to_string()),
            ("piprop", "thumbnail|original|name".to_string()),
            (
                "pithumbsize",
                ARTICLE_IMAGE_FALLBACK_THUMB_WIDTH.to_string(),
            ),
            ("pilicense", "any".to_string()),
            ("imlimit", MEDIA_FILE_LOOKUP_LIMIT.to_string()),
            ("redirects", "1".to_string()),
        ];

        let response: PageMediaResponse = self
            .http
            .get(wiki_api_url(language)?)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let page = response
            .query
            .pages
            .into_iter()
            .next()
            .context("Wikipedia returned no media page")?;

        let mut media = ArticleMedia {
            lead_image: page.thumbnail.map(|thumbnail| MediaItem {
                title: page
                    .pageimage
                    .clone()
                    .unwrap_or_else(|| "Lead image".to_string()),
                url: thumbnail.source,
                fallback_url: None,
                caption: None,
                mime: None,
                width: thumbnail.width,
                height: thumbnail.height,
            }),
            images: Vec::new(),
            audio: None,
        };

        let file_titles = page
            .images
            .unwrap_or_default()
            .into_iter()
            .map(|image| image.title)
            .filter(|title| looks_like_image_or_audio(title) && !looks_like_icon_title(title))
            .take(MEDIA_FILE_LOOKUP_LIMIT)
            .collect::<Vec<_>>();

        if file_titles.is_empty() {
            self.cache_put(cache_key, &media, |cache| &mut cache.article_media);
            return Ok(media);
        }

        let file_infos = self
            .file_infos(language, &file_titles)
            .await
            .unwrap_or_default();
        if media.lead_image.is_none() {
            media.lead_image = file_infos
                .iter()
                .find(|item| {
                    item.mime
                        .as_deref()
                        .is_some_and(|mime| mime.starts_with("image/"))
                })
                .cloned();
        }

        if let Some(lead_image) = &media.lead_image
            && is_gallery_image(lead_image)
        {
            push_unique_media_item(&mut media.images, lead_image.clone());
        }

        for image in file_infos.iter().filter(|item| is_gallery_image(item)) {
            push_unique_media_item(&mut media.images, image.clone());
            if media.images.len() >= ARTICLE_IMAGE_GALLERY_LIMIT {
                break;
            }
        }

        media.audio = file_infos.into_iter().find(|item| {
            item.mime
                .as_deref()
                .is_some_and(|mime| mime.starts_with("audio/"))
        });

        self.cache_put(cache_key, &media, |cache| &mut cache.article_media);
        Ok(media)
    }

    /// Resolves file titles to direct media URLs and dimensions in batches.
    async fn file_infos(&self, language: &str, titles: &[String]) -> Result<Vec<MediaItem>> {
        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("prop", "imageinfo".to_string()),
            ("titles", titles.join("|")),
            ("iiprop", "url|mime|size|mediatype".to_string()),
            ("iiurlwidth", ARTICLE_IMAGE_FALLBACK_THUMB_WIDTH.to_string()),
        ];

        let response: ImageInfoResponse = self
            .http
            .get(wiki_api_url(language)?)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(response
            .query
            .pages
            .into_iter()
            .filter_map(media_item_from_image_info_page)
            .collect())
    }

    /// Reads one typed value from the shared warm-Lambda RAM cache.
    fn cache_get<T, F>(&self, key: &str, select: F) -> Option<T>
    where
        T: Clone,
        F: for<'a> FnOnce(&'a mut RamCache) -> &'a mut HashMap<String, CacheEntry<T>>,
    {
        ram_cache_get(key, select)
    }

    /// Stores one typed value in the shared warm-Lambda RAM cache.
    fn cache_put<T, F>(&self, key: String, value: &T, select: F)
    where
        T: Clone,
        F: for<'a> FnOnce(&'a mut RamCache) -> &'a mut HashMap<String, CacheEntry<T>>,
    {
        ram_cache_put(key, value.clone(), self.cache_max_entries, select);
    }
}

#[derive(Default)]
struct RamCache {
    search: HashMap<String, CacheEntry<Vec<ArticleButton>>>,
    category_search: HashMap<String, CacheEntry<Vec<Category>>>,
    category_articles: HashMap<String, CacheEntry<CategoryArticlePage>>,
    category_subcategories: HashMap<String, CacheEntry<CategorySubcategoryPage>>,
    category_titles: HashMap<String, CacheEntry<String>>,
    category_overviews: HashMap<String, CacheEntry<CategoryOverview>>,
    article_content: HashMap<String, CacheEntry<ArticleContent>>,
    article_categories: HashMap<String, CacheEntry<Vec<Category>>>,
    article_metadata: HashMap<String, CacheEntry<ArticleMetadata>>,
    article_media: HashMap<String, CacheEntry<ArticleMedia>>,
    wikidata_entities: HashMap<String, CacheEntry<WikidataEntity>>,
    wikidata_claims: HashMap<String, CacheEntry<WikidataClaims>>,
    wikidata_labels: HashMap<String, CacheEntry<HashMap<String, WikidataLabelInfo>>>,
    wikidata_property_formatters: HashMap<String, CacheEntry<Option<String>>>,
    wikidata_page_metadata: HashMap<String, CacheEntry<WikidataPageMetadata>>,
}

#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
}

/// Reads a value from the process-global cache used by warm Lambda instances.
fn ram_cache_get<T, F>(key: &str, select: F) -> Option<T>
where
    T: Clone,
    F: for<'a> FnOnce(&'a mut RamCache) -> &'a mut HashMap<String, CacheEntry<T>>,
{
    let mut cache = RAM_CACHE
        .get_or_init(|| Mutex::new(RamCache::default()))
        .lock()
        .expect("RAM cache lock is not poisoned");
    let map = select(&mut cache);
    map.get(key).map(|entry| entry.value.clone())
}

/// Writes a value to the process-global cache and evicts one oldest entry if the
/// configured capacity is exceeded.
fn ram_cache_put<T, F>(key: String, value: T, max_entries: usize, select: F)
where
    F: for<'a> FnOnce(&'a mut RamCache) -> &'a mut HashMap<String, CacheEntry<T>>,
{
    if max_entries == 0 {
        return;
    }

    let mut cache = RAM_CACHE
        .get_or_init(|| Mutex::new(RamCache::default()))
        .lock()
        .expect("RAM cache lock is not poisoned");
    let map = select(&mut cache);

    while map.len() >= max_entries {
        let Some(old_key) = map.keys().next().cloned() else {
            break;
        };
        map.remove(&old_key);
    }

    map.insert(key, CacheEntry { value });
}

/// Spawns a Wikimedia follow-up task on the Tokio runtime.
fn spawn_wiki_task<T>(
    future: impl std::future::Future<Output = Result<T>> + Send + 'static,
) -> JoinHandle<Result<T>>
where
    T: Send + 'static,
{
    tokio::spawn(future)
}

/// Awaits a spawned task and converts join failures into anyhow errors.
async fn join_task<T>(handle: JoinHandle<Result<T>>) -> Result<T> {
    handle.await.context("background task panicked")?
}

/// Builds the MediaWiki API URL for a language code.
fn wiki_api_url(language: &str) -> Result<String> {
    let language = normalize_language_code(language).context("invalid Wikipedia language code")?;
    Ok(format!("https://{language}.wikipedia.org/w/api.php"))
}

/// Reads a required environment variable.
fn required_env(key: &str) -> Result<String> {
    env::var(key).with_context(|| format!("{key} is required"))
}

/// Reads an optional environment variable, treating empty strings as absent.
fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
}

/// Parses an environment variable into a typed value with a default.
fn parse_env<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match optional_env(key) {
        Some(value) => value
            .parse::<T>()
            .with_context(|| format!("{key} has invalid value {value:?}")),
        None => Ok(default),
    }
}

/// Parses a boolean environment variable with common true/false spellings.
fn parse_bool_env(key: &str, default: bool) -> Result<bool> {
    let Some(value) = optional_env(key) else {
        return Ok(default);
    };

    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("{key} must be one of true/false, yes/no, on/off, or 1/0"),
    }
}

/// Parses the optional Telegram user allow-list.
fn parse_allowed_telegram_user_ids() -> Result<HashSet<i64>> {
    let Some(value) = optional_env("ALLOWED_TELEGRAM_USER_IDS") else {
        return Ok(HashSet::new());
    };

    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            let id = value
                .parse::<i64>()
                .with_context(|| format!("invalid Telegram user id {value:?}"))?;
            if id <= 0 {
                bail!("Telegram user id must be positive: {id}");
            }
            Ok(id)
        })
        .collect()
}

/// Normalizes a Wikipedia language code to the conservative format we accept.
fn normalize_language_code(value: &str) -> Option<String> {
    let value = value.trim().to_ascii_lowercase();
    if (2..=16).contains(&value.len())
        && value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
    {
        Some(value)
    } else {
        None
    }
}

/// Parses favorite category definitions from Lambda configuration.
fn parse_favorite_categories(
    value: Option<&str>,
    default_language: &str,
) -> Result<Vec<FavoriteCategory>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };

    let mut favorites = Vec::new();
    let mut seen = HashSet::new();
    for raw_entry in value
        .split([',', '\n', '|'])
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let request = parse_category_value(raw_entry, default_language)?;
        let favorite = FavoriteCategory {
            language: request.language,
            title: request.title,
        };
        let key = format!(
            "{}:{}",
            favorite.language,
            favorite.title.to_ascii_lowercase()
        );
        if seen.insert(key) {
            favorites.push(favorite);
        }
    }

    Ok(favorites)
}

/// Normalizes a category title to the namespace prefix used by a given wiki.
fn normalize_category_title_for_language(value: &str, language: &str) -> String {
    let trimmed = value.trim().replace('_', " ");
    let lower = trimmed.to_lowercase();
    if lower.starts_with("category:")
        || lower.starts_with("категория:")
        || lower.starts_with("катэгорыя:")
        || trimmed.contains(':')
    {
        trimmed
    } else {
        format!("{}:{trimmed}", category_namespace_prefix(language))
    }
}

/// Returns the localized category namespace prefix for the languages we
/// special-case.
fn category_namespace_prefix(language: &str) -> &'static str {
    match language {
        "be" => "Катэгорыя",
        "ru" => "Категория",
        _ => "Category",
    }
}

/// Removes the category namespace for display text.
fn category_display_title(title: &str) -> String {
    title
        .strip_prefix("Category:")
        .or_else(|| title.strip_prefix("Категория:"))
        .or_else(|| title.strip_prefix("Катэгорыя:"))
        .unwrap_or(title)
        .trim()
        .to_string()
}

/// Builds a MediaWiki search query for broad category lookup.
fn category_search_query(query: &str) -> String {
    let query = query.trim();
    if query.split_whitespace().count() == 1 {
        format!("intitle:{query}")
    } else {
        query.to_string()
    }
}

/// Checks whether all category search terms appear in a candidate title.
fn category_title_matches_query(title: &str, query: &str) -> bool {
    let title = category_display_title(title).to_lowercase();
    let query = query.to_lowercase();
    query
        .split_whitespace()
        .filter(|part| !part.is_empty())
        .all(|part| title.contains(part))
}

/// Produces a sort key that prefers broad, useful category matches.
fn category_search_score(
    title: &str,
    query: &str,
    original_rank: usize,
) -> (u8, u8, u8, usize, usize, usize) {
    let display = category_display_title(title);
    let display_key = normalized_title_key(&display);
    let query_key = normalized_title_key(query);
    (
        if display_key == query_key { 0 } else { 1 },
        if is_maintenance_category_title(&display) {
            1
        } else {
            0
        },
        category_specificity_bucket(&display, query),
        display.split_whitespace().count(),
        display.chars().count(),
        original_rank,
    )
}

/// Buckets category titles by how broad or specific they appear for a query.
fn category_specificity_bucket(title: &str, query: &str) -> u8 {
    if normalized_title_key(title) == normalized_title_key(query) {
        return 0;
    }

    let title = title.to_lowercase();
    let title_word_count = title.split_whitespace().count();
    let query_word_count = query
        .split_whitespace()
        .filter(|part| !part.is_empty())
        .count();
    let has_specificity_marker = [
        " about ",
        " by ",
        " from ",
        " in ",
        " of ",
        " at ",
        " for ",
        " related to ",
        " set in ",
        " based on ",
        " featuring ",
    ]
    .iter()
    .any(|needle| title.contains(needle));

    if !has_specificity_marker && title_word_count <= query_word_count.max(2) {
        1
    } else if !has_specificity_marker {
        2
    } else {
        3
    }
}

/// Filters out maintenance categories from user-facing category search ranking.
fn is_maintenance_category_title(title: &str) -> bool {
    let title = title.to_lowercase();
    [
        "wikipedia",
        "wikiproject",
        "articles",
        "requested",
        "templates",
        "stubs",
        "pages",
    ]
    .iter()
    .any(|needle| title.contains(needle))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SearchRequest {
    language: String,
    query: String,
}

/// Parses a free-text Wikipedia search request and optional `lang:` prefix.
fn parse_search_request(text: &str, default_language: &str) -> Result<SearchRequest> {
    let text = text.trim();
    if let Some(rest) = text.strip_prefix("lang:") {
        let mut parts = rest.splitn(2, char::is_whitespace);
        let language = parts
            .next()
            .and_then(normalize_language_code)
            .context("lang:<code> must use a Wikipedia language code")?;
        let query = parts.next().unwrap_or_default().trim();
        if query.is_empty() {
            bail!("search query is empty");
        }
        return Ok(SearchRequest {
            language,
            query: query.to_string(),
        });
    }

    Ok(SearchRequest {
        language: detect_language(text, default_language),
        query: text.to_string(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CategoryRequest {
    language: String,
    title: String,
}

/// Parses `/category` commands and category aliases such as `c games`.
fn parse_category_command(text: &str, default_language: &str) -> Result<Option<CategoryRequest>> {
    let (value, forced_language) = if let Some(rest) = text.strip_prefix("/category") {
        (rest.trim(), None)
    } else if let Some(alias) = strip_category_alias(text) {
        (alias.value.trim(), alias.forced_language)
    } else {
        return Ok(None);
    };

    if value.is_empty() {
        bail!("category command needs a category title");
    }

    Ok(Some(parse_category_value_with_forced_language(
        value,
        default_language,
        forced_language,
    )?))
}

/// Parses one category value from configuration or user input.
fn parse_category_value(value: &str, default_language: &str) -> Result<CategoryRequest> {
    parse_category_value_with_forced_language(value, default_language, None)
}

/// Parses a category value with an optional externally forced language.
fn parse_category_value_with_forced_language(
    value: &str,
    default_language: &str,
    forced_language: Option<&str>,
) -> Result<CategoryRequest> {
    let value = value.trim();
    if value.is_empty() {
        bail!("category title is empty");
    }

    let (language, title) =
        parse_language_prefixed_category_value(value, default_language, forced_language)?;
    Ok(CategoryRequest {
        title: normalize_category_title_for_language(title, &language),
        language,
    })
}

/// Splits a `lang:title` category value while preserving titles that contain
/// colons.
fn parse_language_prefixed_category_value<'a>(
    value: &'a str,
    default_language: &str,
    forced_language: Option<&str>,
) -> Result<(String, &'a str)> {
    let Some((possible_language, rest)) = value.split_once(':') else {
        let language = forced_language
            .map(str::to_string)
            .unwrap_or_else(|| detect_language(value, default_language));
        return Ok((language, value));
    };

    if let Some(language) = normalize_language_code(possible_language) {
        let rest = rest.trim();
        if rest.is_empty() {
            bail!("missing category title after language prefix {possible_language}:");
        }
        Ok((language, rest))
    } else {
        let language = forced_language
            .map(str::to_string)
            .unwrap_or_else(|| detect_language(value, default_language));
        Ok((language, value))
    }
}

struct CategoryAlias<'a> {
    value: &'a str,
    forced_language: Option<&'static str>,
}

/// Recognizes category shortcut prefixes across English, Russian, and Belarusian
/// user input.
fn strip_category_alias(text: &str) -> Option<CategoryAlias<'_>> {
    let text = text.trim();
    let lower = text.to_lowercase();
    for (prefix, forced_language) in [
        ("катэгорыя:", Some("be")),
        ("катэгорыя ", Some("be")),
        ("категория:", Some("ru")),
        ("категория ", Some("ru")),
        ("к:", None),
        ("к ", None),
        ("category:", None),
        ("category ", None),
        ("c:", None),
        ("c ", None),
    ] {
        if lower.starts_with(prefix) {
            return text.get(prefix.len()..).map(|value| CategoryAlias {
                value,
                forced_language,
            });
        }
    }
    None
}

/// Chooses a likely Wikipedia language from the script used in a user query.
fn detect_language(text: &str, default_language: &str) -> String {
    if text
        .chars()
        .any(|character| matches!(character, '\u{10A0}'..='\u{10FF}'))
    {
        "ka".to_string()
    } else if text
        .chars()
        .any(|character| matches!(character, 'І' | 'і' | 'Ў' | 'ў'))
    {
        "be".to_string()
    } else if text
        .chars()
        .any(|character| matches!(character, '\u{0400}'..='\u{052F}'))
    {
        "ru".to_string()
    } else if text
        .chars()
        .any(|character| matches!(character, '\u{0370}'..='\u{03FF}'))
    {
        "el".to_string()
    } else if text
        .chars()
        .any(|character| matches!(character, '\u{0590}'..='\u{05FF}'))
    {
        "he".to_string()
    } else if text
        .chars()
        .any(|character| matches!(character, '\u{0600}'..='\u{06FF}'))
    {
        "ar".to_string()
    } else if text
        .chars()
        .any(|character| matches!(character, '\u{0530}'..='\u{058F}'))
    {
        "hy".to_string()
    } else if text
        .chars()
        .any(|character| matches!(character, '\u{3040}'..='\u{30FF}'))
    {
        "ja".to_string()
    } else if text
        .chars()
        .any(|character| matches!(character, '\u{AC00}'..='\u{D7AF}'))
    {
        "ko".to_string()
    } else if text
        .chars()
        .any(|character| matches!(character, '\u{4E00}'..='\u{9FFF}'))
    {
        "zh".to_string()
    } else if text
        .chars()
        .any(|character| matches!(character, '\u{0900}'..='\u{097F}'))
    {
        "hi".to_string()
    } else if text
        .chars()
        .any(|character| matches!(character, '\u{0E00}'..='\u{0E7F}'))
    {
        "th".to_string()
    } else {
        default_language.to_string()
    }
}

enum CallbackAction {
    Article {
        language: String,
        page_id: u64,
    },
    Category {
        language: String,
        title: String,
    },
    CategoryPage {
        language: String,
        page_id: u64,
        kind: CategoryPageKind,
        page_index: usize,
        total_pages: Option<usize>,
    },
    ArticleCategory {
        language: String,
        page_id: u64,
        index: usize,
    },
    Wikidata {
        language: String,
        item: String,
    },
    Images {
        language: String,
        page_id: u64,
    },
}

/// Decodes the compact callback payloads used by Telegram buttons.
fn parse_callback_data(
    data: &str,
    favorite_categories: &[FavoriteCategory],
) -> Result<CallbackAction> {
    if let Some(rest) = data.strip_prefix("article:") {
        return parse_article_callback(rest);
    }

    if let Some(rest) = data.strip_prefix("catpage:") {
        return parse_category_page_callback(rest);
    }

    if let Some(rest) = data.strip_prefix("article_category:") {
        return parse_article_category_callback(rest);
    }

    if let Some(rest) = data.strip_prefix("wikidata:") {
        return parse_wikidata_callback(rest);
    }

    if let Some(rest) = data.strip_prefix("images:") {
        return parse_images_callback(rest);
    }

    if let Some(rest) = data.strip_prefix("category:") {
        return parse_category_callback(rest);
    }

    if let Some(rest) = data.strip_prefix("favorite_category:") {
        return parse_favorite_category_callback(rest, favorite_categories);
    }

    bail!("unknown callback data")
}

/// Decodes an article button callback.
fn parse_article_callback(rest: &str) -> Result<CallbackAction> {
    let mut parts = rest.split(':');
    let language = parse_callback_language(&mut parts, "article")?;
    let page_id = parse_callback_page_id(&mut parts, "article")?;
    ensure_no_callback_fields(&mut parts, "article")?;
    Ok(CallbackAction::Article { language, page_id })
}

/// Decodes a paginated category callback.
fn parse_category_page_callback(rest: &str) -> Result<CallbackAction> {
    let mut parts = rest.split(':');
    let language = parse_callback_language(&mut parts, "category page")?;
    let page_id = parse_callback_page_id(&mut parts, "category page")?;
    let kind = parts
        .next()
        .and_then(CategoryPageKind::from_callback_code)
        .context("category page callback has invalid kind")?;
    let page_index = parts
        .next()
        .context("category page callback has no page index")?
        .parse::<usize>()
        .context("category page callback page index is invalid")?;
    let total_pages = parts
        .next()
        .map(|value| {
            value
                .parse::<usize>()
                .context("category page callback total page count is invalid")
        })
        .transpose()?;
    ensure_no_callback_fields(&mut parts, "category page")?;
    Ok(CallbackAction::CategoryPage {
        language,
        page_id,
        kind,
        page_index,
        total_pages,
    })
}

/// Decodes a callback for one category attached to an article.
fn parse_article_category_callback(rest: &str) -> Result<CallbackAction> {
    let mut parts = rest.split(':');
    let language = parse_callback_language(&mut parts, "article category")?;
    let page_id = parse_callback_page_id(&mut parts, "article category")?;
    let index = parts
        .next()
        .context("article category callback has no index")?
        .parse::<usize>()
        .context("article category callback index is invalid")?;
    ensure_no_callback_fields(&mut parts, "article category")?;
    Ok(CallbackAction::ArticleCategory {
        language,
        page_id,
        index,
    })
}

/// Decodes a Wikidata metadata callback.
fn parse_wikidata_callback(rest: &str) -> Result<CallbackAction> {
    let mut parts = rest.split(':');
    let language = parse_callback_language(&mut parts, "Wikidata")?;
    let item = parts
        .next()
        .and_then(normalize_wikidata_entity_id)
        .context("Wikidata callback has invalid item id")?;
    ensure_no_callback_fields(&mut parts, "Wikidata")?;
    Ok(CallbackAction::Wikidata { language, item })
}

/// Decodes an article image gallery callback.
fn parse_images_callback(rest: &str) -> Result<CallbackAction> {
    let mut parts = rest.split(':');
    let language = parse_callback_language(&mut parts, "images")?;
    let page_id = parse_callback_page_id(&mut parts, "images")?;
    ensure_no_callback_fields(&mut parts, "images")?;
    Ok(CallbackAction::Images { language, page_id })
}

/// Decodes a category-title callback whose title is base64url encoded.
fn parse_category_callback(rest: &str) -> Result<CallbackAction> {
    let mut parts = rest.split(':');
    let language = parse_callback_language(&mut parts, "category")?;
    let encoded = parts.next().context("category callback has no title")?;
    ensure_no_callback_fields(&mut parts, "category")?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .context("category callback title is not base64url")?;
    let title = String::from_utf8(bytes).context("category callback title is not utf-8")?;
    Ok(CallbackAction::Category {
        title: normalize_category_title_for_language(&title, &language),
        language,
    })
}

/// Decodes a callback pointing at a configured favorite category.
fn parse_favorite_category_callback(
    rest: &str,
    favorite_categories: &[FavoriteCategory],
) -> Result<CallbackAction> {
    let index = rest
        .parse::<usize>()
        .context("favorite category callback index is invalid")?;
    let favorite = favorite_categories
        .get(index)
        .context("favorite category index is out of range")?;
    Ok(CallbackAction::Category {
        language: favorite.language.clone(),
        title: favorite.title.clone(),
    })
}

/// Parses the language field shared by most callbacks.
fn parse_callback_language(
    parts: &mut std::str::Split<'_, char>,
    callback_name: &str,
) -> Result<String> {
    parts
        .next()
        .and_then(normalize_language_code)
        .with_context(|| format!("{callback_name} callback has invalid language"))
}

/// Parses the page id field shared by article and category-page callbacks.
fn parse_callback_page_id(
    parts: &mut std::str::Split<'_, char>,
    callback_name: &str,
) -> Result<u64> {
    parts
        .next()
        .with_context(|| format!("{callback_name} callback has no page id"))?
        .parse::<u64>()
        .with_context(|| format!("{callback_name} callback page id is invalid"))
}

/// Rejects callback payloads with extra colon-separated fields.
fn ensure_no_callback_fields(
    parts: &mut std::str::Split<'_, char>,
    callback_name: &str,
) -> Result<()> {
    if parts.next().is_some() {
        bail!("{callback_name} callback has extra fields");
    }
    Ok(())
}

/// Builds Telegram inline query result cards for article suggestions.
fn inline_query_results(language: &str, results: &[ArticleButton]) -> Vec<Value> {
    results
        .iter()
        .take(MAX_SEARCH_LIMIT)
        .map(|result| {
            let url = article_url(language, &result.title);
            json!({
                "type": "article",
                "id": format!("{language}:{}", result.page_id),
                "title": truncate_for_telegram(&result.title, 128),
                "description": inline_result_description(language, result),
                "url": url,
                "hide_url": false,
                "input_message_content": {
                    "message_text": inline_message_text(language, result, &url),
                    "parse_mode": "HTML",
                    "disable_web_page_preview": false,
                },
                "reply_markup": {
                    "inline_keyboard": [[{
                        "text": "Open on Wikipedia",
                        "url": url,
                    }]]
                },
            })
        })
        .collect()
}

/// Builds the HTML inserted into chat when a user selects an inline result.
fn inline_message_text(language: &str, result: &ArticleButton, url: &str) -> String {
    let title = html_bold(&result.title);
    let url_link = html_link(url, url);
    let available = INLINE_MESSAGE_CHAR_LIMIT
        .saturating_sub(title.chars().count())
        .saturating_sub(url_link.chars().count())
        .saturating_sub(4)
        .max(1);
    let snippet = result
        .snippet
        .as_deref()
        .map(str::trim)
        .filter(|snippet| !snippet.is_empty())
        .map(|snippet| html_escape_text(&truncate_for_telegram(snippet, available)))
        .unwrap_or_else(|| html_escape_text(&inline_result_description(language, result)));

    format!("{title}\n\n{snippet}\n\n{url_link}")
}

/// Builds a short plain-text inline result description.
fn inline_result_description(language: &str, result: &ArticleButton) -> String {
    let mut parts = vec![format!("{language}.wikipedia.org")];
    if let Some(snippet) = result
        .snippet
        .as_deref()
        .map(str::trim)
        .filter(|snippet| !snippet.is_empty())
    {
        return truncate_for_telegram(snippet, 240);
    }
    if let Some(wordcount) = result.wordcount {
        parts.push(format!("{wordcount} words"));
    }
    if let Some(size) = result.size {
        parts.push(format!("{size} bytes"));
    }
    parts.join(" | ")
}

/// Normalizes MediaWiki plaintext extracts while preserving paragraph breaks.
fn normalize_inline_extract(value: &str) -> String {
    value
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Builds one-button-per-row article keyboards, including size and
/// disambiguation markers.
fn article_results_keyboard(
    language: &str,
    results: &[ArticleButton],
    big_article_char_threshold: usize,
    small_article_char_threshold: usize,
    highlight_as_primary: bool,
) -> Vec<Vec<Value>> {
    results
        .iter()
        .map(|result| {
            let mut label = result.title.clone();
            if result.is_disambiguation {
                label = format!("{DISAMBIGUATION_BUTTON_PREFIX}{label}");
            }
            if let Some(wordcount) = result.wordcount {
                label.push_str(&format!(" ({wordcount} words)"));
            }
            label = truncate_for_telegram(&label, TELEGRAM_MAX_BUTTON_TEXT);

            let mut button = json!({
                "text": label,
                "callback_data": format!("article:{language}:{}", result.page_id),
            });

            let style = if result
                .size
                .is_some_and(|size| size >= big_article_char_threshold)
            {
                Some("danger")
            } else if highlight_as_primary
                || result
                    .size
                    .is_some_and(|size| size <= small_article_char_threshold)
            {
                Some("primary")
            } else {
                None
            };

            if let Some(style) = style {
                button
                    .as_object_mut()
                    .expect("button is a JSON object")
                    .insert("style".to_string(), Value::String(style.to_string()));
            }

            vec![button]
        })
        .collect()
}

/// Builds the favorite category keyboard shown in help/start responses.
fn favorite_categories_keyboard(favorites: &[FavoriteCategory]) -> Option<Value> {
    if favorites.is_empty() {
        return None;
    }

    let rows = favorites
        .iter()
        .enumerate()
        .map(|(index, favorite)| {
            vec![json!({
                "text": truncate_for_telegram(
                    &format!(
                        "{}: {}",
                        favorite.language,
                        category_display_title(&favorite.title)
                    ),
                    TELEGRAM_MAX_BUTTON_TEXT
                ),
                "callback_data": format!("favorite_category:{index}"),
            })]
        })
        .collect::<Vec<_>>();

    Some(json!({ "inline_keyboard": rows }))
}

/// Builds category search result buttons.
fn category_search_keyboard(language: &str, categories: &[Category]) -> Option<Value> {
    let rows = category_buttons_rows(language, categories, None);
    if rows.is_empty() {
        return None;
    }

    Some(json!({ "inline_keyboard": rows }))
}

/// Builds subcategory buttons plus optional pagination controls.
fn category_subcategories_keyboard_with_pagination(
    language: &str,
    subcategories: &[Category],
    pagination: Option<CategoryPagination<'_>>,
) -> Option<Value> {
    let rows = category_buttons_rows(
        language,
        subcategories,
        subcategory_button_style(subcategories.len()),
    );
    let rows = with_category_pagination_row(rows, pagination);
    if rows.is_empty() {
        return None;
    }

    Some(json!({ "inline_keyboard": rows }))
}

/// Chooses Telegram button style hints based on subcategory count.
fn subcategory_button_style(count: usize) -> Option<&'static str> {
    if count > 10 {
        Some("danger")
    } else if count == 1 {
        Some("primary")
    } else {
        None
    }
}

/// Builds rows of category callback buttons.
fn category_buttons_rows(
    language: &str,
    categories: &[Category],
    style: Option<&str>,
) -> Vec<Vec<Value>> {
    categories
        .iter()
        .filter_map(|category| {
            category_title_callback_data(language, &category.title).map(|callback_data| {
                let mut button = json!({
                    "text": truncate_for_telegram(
                        &category_display_title(&category.title),
                        TELEGRAM_MAX_BUTTON_TEXT
                    ),
                    "callback_data": callback_data,
                });

                if let Some(style) = style {
                    button
                        .as_object_mut()
                        .expect("button is a JSON object")
                        .insert("style".to_string(), Value::String(style.to_string()));
                }

                button
            })
        })
        .collect::<Vec<_>>()
        .chunks(2)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>()
}

/// Builds article category buttons for a source article.
fn article_categories_keyboard(
    language: &str,
    page_id: u64,
    categories: &[Category],
) -> Option<Value> {
    if categories.is_empty() {
        return None;
    }

    let buttons = categories
        .iter()
        .take(CATEGORY_BUTTON_LIMIT)
        .enumerate()
        .map(|(index, category)| category_button(language, page_id, index, category))
        .collect::<Vec<_>>();

    let rows = buttons
        .chunks(2)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>();

    Some(json!({ "inline_keyboard": rows }))
}

/// Builds category article buttons plus optional pagination controls.
fn category_articles_keyboard(
    language: &str,
    articles: &[ArticleButton],
    big_article_char_threshold: usize,
    small_article_char_threshold: usize,
    highlight_as_primary: bool,
    pagination: Option<CategoryPagination<'_>>,
) -> Option<Value> {
    let rows = article_results_keyboard(
        language,
        articles,
        big_article_char_threshold,
        small_article_char_threshold,
        highlight_as_primary,
    );
    let rows = with_category_pagination_row(rows, pagination);
    (!rows.is_empty()).then(|| json!({ "inline_keyboard": rows }))
}

#[derive(Clone, Copy)]
struct CategoryPagination<'a> {
    language: &'a str,
    page_id: u64,
    kind: CategoryPageKind,
    page_index: usize,
    total_pages: Option<usize>,
    has_next: bool,
}

/// Appends a Prev/Next row to category member keyboards.
fn with_category_pagination_row(
    mut rows: Vec<Vec<Value>>,
    pagination: Option<CategoryPagination<'_>>,
) -> Vec<Vec<Value>> {
    if let Some(row) = pagination.and_then(category_pagination_row) {
        rows.push(row);
    }
    rows
}

/// Builds Prev/Next buttons for paginated category pages.
fn category_pagination_row(pagination: CategoryPagination<'_>) -> Option<Vec<Value>> {
    let mut row = Vec::new();
    if pagination.page_index > 0
        && let Some(callback_data) = category_page_callback_data(
            pagination.language,
            pagination.page_id,
            pagination.kind,
            pagination.page_index - 1,
            pagination.total_pages,
        )
    {
        row.push(json!({
            "text": "Prev",
            "callback_data": callback_data,
        }));
    }
    if pagination.has_next
        && let Some(callback_data) = category_page_callback_data(
            pagination.language,
            pagination.page_id,
            pagination.kind,
            pagination.page_index + 1,
            pagination.total_pages,
        )
    {
        row.push(json!({
            "text": "Next",
            "callback_data": callback_data,
        }));
    }

    (!row.is_empty()).then_some(row)
}

/// Builds one category callback button, falling back to article-local category
/// callbacks if the title is too long for Telegram callback data.
fn category_button(language: &str, page_id: u64, index: usize, category: &Category) -> Value {
    let title = &category.title;
    let text = truncate_for_telegram(&category_display_title(title), TELEGRAM_MAX_BUTTON_TEXT);
    let mut button = json!({
        "text": text,
        "callback_data": category_title_callback_data(language, title)
            .unwrap_or_else(|| article_category_callback_data(language, page_id, index)),
    });

    if let Some(style) = article_category_button_style(category.article_count) {
        button
            .as_object_mut()
            .expect("button is a JSON object")
            .insert("style".to_string(), Value::String(style.to_string()));
    }

    button
}

/// Chooses Telegram button style hints based on article counts in a category.
fn article_category_button_style(article_count: Option<usize>) -> Option<&'static str> {
    match article_count {
        Some(1) => Some("primary"),
        Some(count) if count > 10 => Some("danger"),
        _ => None,
    }
}

/// Builds the Wikidata button shown in article metadata.
fn wikidata_metadata_keyboard(metadata: &ArticleMetadata) -> Option<Value> {
    let item = metadata.info.page_props.get("wikibase_item")?;
    let callback_data = wikidata_callback_data(&metadata.language, item)?;
    let property_count = metadata.wikidata_property_count;
    let mut button = json!({
        "text": truncate_for_telegram(
            &match property_count {
                Some(count) => format!("Wikidata ({count} properties)"),
                None => "Wikidata".to_string(),
            },
            TELEGRAM_MAX_BUTTON_TEXT
        ),
        "callback_data": callback_data,
    });

    if let Some(style) = wikidata_property_button_style(property_count) {
        button
            .as_object_mut()
            .expect("button is a JSON object")
            .insert("style".to_string(), Value::String(style.to_string()));
    }

    Some(json!({ "inline_keyboard": [[button]] }))
}

/// Chooses Telegram button style hints for Wikidata property counts.
fn wikidata_property_button_style(property_count: Option<usize>) -> Option<&'static str> {
    match property_count {
        Some(count) if count < 5 => Some("primary"),
        Some(count) if count > 10 => Some("danger"),
        _ => None,
    }
}

/// Encodes a Wikidata callback payload within Telegram's callback size limits.
fn wikidata_callback_data(language: &str, item: &str) -> Option<String> {
    let item = normalize_wikidata_entity_id(item)?;
    let data = format!("wikidata:{language}:{item}");
    (data.len() <= 64).then_some(data)
}

/// Encodes category pagination callback data.
fn category_page_callback_data(
    language: &str,
    page_id: u64,
    kind: CategoryPageKind,
    page_index: usize,
    total_pages: Option<usize>,
) -> Option<String> {
    let language = normalize_language_code(language)?;
    let data = match total_pages {
        Some(total_pages) => format!(
            "catpage:{language}:{page_id}:{}:{page_index}:{total_pages}",
            kind.callback_code()
        ),
        None => format!(
            "catpage:{language}:{page_id}:{}:{page_index}",
            kind.callback_code()
        ),
    };
    (data.len() <= 64).then_some(data)
}

/// Builds the button that lets users request article images on demand.
fn article_images_keyboard(language: &str, page_id: u64, image_count: usize) -> Option<Value> {
    if image_count == 0 {
        return None;
    }

    let callback_data = article_images_callback_data(language, page_id)?;
    let button = json!({
        "text": truncate_for_telegram(
            &format!("Images ({})", image_count.min(ARTICLE_IMAGE_GALLERY_LIMIT)),
            TELEGRAM_MAX_BUTTON_TEXT
        ),
        "callback_data": callback_data,
    });

    Some(json!({ "inline_keyboard": [[button]] }))
}

/// Encodes article image callback data.
fn article_images_callback_data(language: &str, page_id: u64) -> Option<String> {
    let language = normalize_language_code(language)?;
    let data = format!("images:{language}:{page_id}");
    (data.len() <= 64).then_some(data)
}

/// Encodes category title callback data with base64url to keep separators safe.
fn category_title_callback_data(language: &str, title: &str) -> Option<String> {
    let encoded = URL_SAFE_NO_PAD.encode(normalize_category_title_for_language(title, language));
    let data = format!("category:{language}:{encoded}");
    (data.len() <= 64).then_some(data)
}

/// Encodes an article-local category index callback.
fn article_category_callback_data(language: &str, page_id: u64, index: usize) -> String {
    format!("article_category:{language}:{page_id}:{index}")
}

/// Builds a canonical Wikipedia article URL for a title.
fn article_url(language: &str, title: &str) -> String {
    format!(
        "https://{language}.wikipedia.org/wiki/{}",
        urlencoding::encode(&title.replace(' ', "_"))
    )
}

/// Renders an article into Telegram HTML message chunks.
#[cfg(test)]
fn render_article_html_parts(
    language: &str,
    article: &ArticleContent,
    limit: usize,
) -> Vec<String> {
    render_article_output_parts(language, article, limit)
        .into_iter()
        .filter_map(|part| match part {
            ArticleOutputPart::Html(html) => Some(html),
            ArticleOutputPart::Images(_) => None,
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ArticleOutputPart {
    Html(String),
    Images(Vec<MediaItem>),
}

/// Renders article output into text and inline image parts so images can be sent
/// between section messages.
fn render_article_output_parts(
    language: &str,
    article: &ArticleContent,
    limit: usize,
) -> Vec<ArticleOutputPart> {
    let mut intro_sections = Vec::new();
    intro_sections.push(html_bold_link(
        &article_url(language, &article.title),
        &article.title,
    ));

    if article.is_disambiguation {
        intro_sections.push(format!(
            "{}\n{}",
            html_bold("Disambiguation page"),
            html_escape_text("Choose a target article from the buttons below.")
        ));
        return combine_html_sections(intro_sections, limit)
            .into_iter()
            .map(ArticleOutputPart::Html)
            .collect();
    }

    if let Some(infobox) = &article.infobox {
        intro_sections.push(format!(
            "{}\n{}",
            html_bold("Infobox"),
            render_infobox_html(infobox, article_infobox_limit(article.length))
        ));
    }

    let mut parts = combine_html_sections(intro_sections, limit)
        .into_iter()
        .map(ArticleOutputPart::Html)
        .collect::<Vec<_>>();
    if let Some(image) = article.infobox_image.as_ref() {
        parts.push(ArticleOutputPart::Images(vec![image.clone()]));
    }

    let fallback_sections;
    let body_sections = if !article.body_sections.is_empty() {
        article.body_sections.as_slice()
    } else if let Some(body_html) = article
        .body_html
        .as_deref()
        .map(str::trim)
        .filter(|body| !body.is_empty())
    {
        fallback_sections = vec![ArticleBodySection {
            heading: None,
            html: body_html.to_string(),
            images: Vec::new(),
        }];
        fallback_sections.as_slice()
    } else {
        let extract = article.extract.trim();
        let extract = if extract.is_empty() {
            "Wikipedia did not return a plain-text extract for this page."
        } else {
            extract
        };
        fallback_sections = vec![ArticleBodySection {
            heading: None,
            html: html_escape_text(extract),
            images: Vec::new(),
        }];
        fallback_sections.as_slice()
    };

    for section in body_sections {
        parts.extend(
            render_article_body_section_parts(
                language,
                &article.title,
                std::slice::from_ref(section),
                limit,
            )
            .into_iter()
            .map(ArticleOutputPart::Html),
        );
        if !section.images.is_empty() {
            parts.push(ArticleOutputPart::Images(section.images.clone()));
        }
    }

    parts
}

/// Renders one article section into Telegram-sized message chunks.
fn render_article_body_section_parts(
    language: &str,
    article_title: &str,
    sections: &[ArticleBodySection],
    limit: usize,
) -> Vec<String> {
    let limit = limit.max(1);
    let mut parts = Vec::new();

    for section in sections {
        let section_html = section.html.trim();
        if section_html.is_empty() {
            continue;
        }

        let prefix = render_article_body_section_prefix(language, article_title, section);
        let prefix_len = prefix.chars().count();
        let chunk_limit = limit.saturating_sub(prefix_len + 2).max(1);

        for chunk in split_html_lines_for_telegram(section_html, chunk_limit) {
            let chunk = chunk.trim();
            if chunk.is_empty() {
                parts.push(prefix.clone());
            } else {
                parts.push(format!("{prefix}\n\n{chunk}"));
            }
        }
    }

    parts
}

/// Builds the clickable article/section prefix used at the start of each
/// article body chunk.
fn render_article_body_section_prefix(
    language: &str,
    article_title: &str,
    section: &ArticleBodySection,
) -> String {
    let article_url = article_url(language, article_title);
    let article = html_link(&article_url, article_title);
    match section.heading.as_ref() {
        Some(heading) => format!(
            "{}: {}",
            article,
            html_italic_link(
                &article_body_section_url(language, article_title, heading.anchor.as_deref()),
                &heading.title
            )
        ),
        None => article,
    }
}

/// Builds the section URL, including a fragment when MediaWiki exposes one.
fn article_body_section_url(language: &str, article_title: &str, anchor: Option<&str>) -> String {
    let url = article_url(language, article_title);
    anchor
        .map(|anchor| format!("{url}#{anchor}"))
        .unwrap_or(url)
}

/// Renders article categories as one clickable category link per line.
fn render_categories_html(language: &str, categories: &[Category]) -> String {
    let category_names = categories
        .iter()
        .take(CATEGORY_BUTTON_LIMIT)
        .map(|category| {
            html_link(
                &article_url(language, &category.title),
                &category_display_title(&category.title),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("{}\n{}", html_bold("Categories"), category_names)
}

/// Renders parent categories for a category page.
fn render_parent_categories_html(language: &str, title: &str, categories: &[Category]) -> String {
    let category_names = categories
        .iter()
        .map(|category| {
            html_link(
                &article_url(language, &category.title),
                &category_display_title(&category.title),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "{}\n{}",
        html_bold(&format!(
            "Parent categories of {}",
            normalize_category_title_for_language(title, language)
        )),
        category_names
    )
}

/// Renders clickable names for category search results.
fn render_category_search_html(language: &str, query: &str, categories: &[Category]) -> String {
    let category_names = categories
        .iter()
        .map(|category| {
            html_link(
                &article_url(language, &category.title),
                &category_display_title(&category.title),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "{}\n{}",
        html_bold(&format!("Categories matching {query}")),
        category_names
    )
}

/// Renders category descriptions into Telegram-sized HTML chunks.
fn render_category_description_html_parts(
    language: &str,
    title: &str,
    description: &str,
    limit: usize,
) -> Vec<String> {
    let title = normalize_category_title_for_language(title, language);
    let heading = html_bold_link(&article_url(language, &title), &title);
    let description = html_escape_text(description);
    combine_html_sections(vec![format!("{heading}\n\n{description}")], limit)
}

/// Renders a subcategory page heading with optional page count.
fn render_category_subcategories_page_heading_html(
    language: &str,
    title: &str,
    page_index: usize,
    total_pages: Option<usize>,
) -> String {
    render_category_page_heading_html(language, title, "Subcategories in", page_index, total_pages)
}

/// Renders a category article page heading with optional page count.
fn render_category_articles_page_heading_html(
    language: &str,
    title: &str,
    page_index: usize,
    total_pages: Option<usize>,
) -> String {
    render_category_page_heading_html(
        language,
        title,
        "Newest article pages in",
        page_index,
        total_pages,
    )
}

/// Renders a category page heading with a clickable category title.
fn render_category_page_heading_html(
    language: &str,
    title: &str,
    prefix: &str,
    page_index: usize,
    total_pages: Option<usize>,
) -> String {
    let title = normalize_category_title_for_language(title, language);
    let linked_title = html_link(&article_url(language, &title), &title);
    let page_number = page_index + 1;
    match total_pages {
        Some(total_pages) if total_pages > 1 || page_index > 0 => {
            format!(
                "{prefix} {linked_title} (page {page_number}/{})",
                total_pages.max(page_number)
            )
        }
        _ if page_index > 0 => format!("{prefix} {linked_title} (page {page_number})"),
        _ => format!("{prefix} {linked_title}"),
    }
}

/// Converts item counts and page size into a total page count.
fn category_total_pages(total_items: Option<usize>, page_size: usize) -> Option<usize> {
    let page_size = page_size.clamp(1, MAX_SEARCH_LIMIT);
    total_items.map(|total_items| total_items.div_ceil(page_size))
}

/// Splits rendered reference HTML into Telegram-sized messages.
fn render_references_html_parts(references_html: &str, limit: usize) -> Vec<String> {
    combine_html_sections(
        vec![format!("{}\n{}", html_bold("References"), references_html)],
        limit,
    )
}

/// Renders a navigation template heading, linking the template title when
/// possible.
fn render_navigation_heading_html(template: &NavTemplateButtons) -> String {
    if let Some(url) = template.title_url.as_deref() {
        format!(
            "<b>Navigation: <a href=\"{}\">{}</a></b>",
            html_escape::encode_double_quoted_attribute(url),
            html_escape_text(&template.title)
        )
    } else {
        html_bold(&format!("Navigation: {}", template.title))
    }
}

/// Renders an infobox under a conservative per-article length cap.
fn render_infobox_html(infobox: &str, limit: usize) -> String {
    split_html_lines_for_telegram(infobox, limit)
        .into_iter()
        .next()
        .unwrap_or_default()
        .lines()
        .map(|line| {
            let line = line.trim();
            parse_coordinate_pair(line)
                .map(|(lat, lon)| html_link(&coordinate_url(lat, lon), line))
                .unwrap_or_else(|| line.to_string())
        })
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Combines logical HTML sections into Telegram-sized message chunks.
fn combine_html_sections(sections: Vec<String>, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    let mut parts = Vec::new();
    let mut current = String::new();

    for section in sections {
        let separator = if current.is_empty() { "" } else { "\n\n" };
        if current.chars().count() + separator.chars().count() + section.chars().count() <= limit {
            current.push_str(separator);
            current.push_str(&section);
            continue;
        }

        if !current.is_empty() {
            parts.push(current);
            current = String::new();
        }

        if section.chars().count() > limit {
            parts.extend(split_html_lines_for_telegram(&section, limit));
        } else {
            current = section;
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

/// Escapes and wraps text in Telegram-compatible bold tags.
fn html_bold(value: &str) -> String {
    format!("<b>{}</b>", html_escape_text(value))
}

/// Builds a Telegram-compatible HTML link.
fn html_link(url: &str, text: &str) -> String {
    format!(
        "<a href=\"{}\">{}</a>",
        html_escape::encode_double_quoted_attribute(url),
        html_escape_text(text)
    )
}

/// Builds a bold Telegram HTML link.
fn html_bold_link(url: &str, text: &str) -> String {
    format!(
        "<b><a href=\"{}\">{}</a></b>",
        html_escape::encode_double_quoted_attribute(url),
        html_escape_text(text)
    )
}

/// Builds an italic Telegram HTML link.
fn html_italic_link(url: &str, text: &str) -> String {
    format!(
        "<i><a href=\"{}\">{}</a></i>",
        html_escape::encode_double_quoted_attribute(url),
        html_escape_text(text)
    )
}

/// Escapes text for Telegram HTML.
fn html_escape_text(value: &str) -> String {
    html_escape::encode_text(value).to_string()
}

/// Normalizes image captions before sending them to Telegram.
fn caption_html_for_telegram(caption: &str) -> String {
    if caption.chars().count() <= 1024 {
        caption.to_string()
    } else {
        html_escape_text(&truncate_for_telegram(&html_to_plain_text(caption), 1024))
    }
}

/// Adds a caption to a Telegram media payload when one is available.
fn add_media_caption_payload(payload: &mut Value, image: &MediaItem) {
    if let Some(caption) = image.caption.as_ref() {
        payload["caption"] = Value::String(caption_html_for_telegram(caption));
        payload["parse_mode"] = Value::String("HTML".to_string());
    }
}

/// Converts MediaWiki HTML into one Telegram HTML body string.
#[cfg(test)]
fn mediawiki_html_to_telegram_html(language: &str, title: &str, html: &str) -> Option<String> {
    article_body_sections_to_html(
        language,
        title,
        &mediawiki_html_to_telegram_sections(language, title, html),
    )
}

/// Joins parsed article sections into the legacy single-body representation.
fn article_body_sections_to_html(
    language: &str,
    title: &str,
    sections: &[ArticleBodySection],
) -> Option<String> {
    let blocks = sections
        .iter()
        .flat_map(|section| {
            let mut blocks = Vec::new();
            if let Some(heading) = &section.heading {
                blocks.push(article_body_heading_html(language, title, heading));
            }
            if !section.html.trim().is_empty() {
                blocks.push(section.html.clone());
            }
            blocks
        })
        .collect::<Vec<_>>();

    (!blocks.is_empty()).then(|| blocks.join("\n\n"))
}

/// Renders an article heading as clickable Telegram HTML.
fn article_body_heading_html(
    language: &str,
    article_title: &str,
    heading: &ArticleBodyHeading,
) -> String {
    heading
        .anchor
        .as_deref()
        .map(|anchor| {
            html_bold_link(
                &article_body_section_url(language, article_title, Some(anchor)),
                &heading.title,
            )
        })
        .unwrap_or_else(|| html_bold(&heading.title))
}

/// Converts MediaWiki HTML into article sections with headings and inline
/// images.
fn mediawiki_html_to_telegram_sections(
    language: &str,
    _title: &str,
    html: &str,
) -> Vec<ArticleBodySection> {
    let document = Html::parse_fragment(html);
    let root = document.root_element();
    let mut sections = ArticleBodySectionBuilder::new();
    collect_html_blocks(language, root, &mut sections);
    sections.finish()
}

struct ArticleBodySectionBuilder {
    sections: Vec<ArticleBodySection>,
    current_heading: Option<ArticleBodyHeading>,
    current_blocks: Vec<String>,
    current_images: Vec<MediaItem>,
}

impl ArticleBodySectionBuilder {
    /// Creates an empty article section collector.
    fn new() -> Self {
        Self {
            sections: Vec::new(),
            current_heading: None,
            current_blocks: Vec::new(),
            current_images: Vec::new(),
        }
    }

    /// Adds a rendered block to the current section.
    fn push_block(&mut self, block: String) {
        self.current_blocks.push(block);
    }

    /// Adds an image to the current section so it can be sent after that text.
    fn push_image(&mut self, image: MediaItem) {
        push_unique_media_item(&mut self.current_images, image);
    }

    /// Starts a new article section.
    fn start_section(&mut self, heading: ArticleBodyHeading) {
        self.flush();
        self.current_heading = Some(heading);
    }

    /// Flushes the current section and returns all collected sections.
    fn finish(mut self) -> Vec<ArticleBodySection> {
        self.flush();
        self.sections
    }

    /// Flushes the current section if it contains text or images.
    fn flush(&mut self) {
        if self.current_blocks.is_empty() && self.current_images.is_empty() {
            return;
        }

        self.sections.push(ArticleBodySection {
            heading: self.current_heading.take(),
            html: self.current_blocks.join("\n\n"),
            images: std::mem::take(&mut self.current_images),
        });
        self.current_blocks.clear();
    }
}

/// Extracts references into a separate Telegram HTML message body.
fn mediawiki_references_to_telegram_html(language: &str, html: &str) -> Option<String> {
    let document = Html::parse_fragment(html);
    let selector = Selector::parse("ol.references li").expect("references selector is valid");
    let lines = document
        .select(&selector)
        .filter_map(|item| render_reference_item(language, item))
        .enumerate()
        .map(|(index, rendered)| format!("{}. {rendered}", index + 1))
        .collect::<Vec<_>>();

    (!lines.is_empty()).then(|| lines.join("\n\n"))
}

/// Detects MediaWiki disambiguation pages from page properties or fallback HTML
/// markers.
fn is_disambiguation_page(parsed: &ParsedPage, html: &str) -> bool {
    parsed
        .properties
        .as_ref()
        .is_some_and(|properties| properties.contains_key("disambiguation"))
        || html.contains("id=\"disambigbox\"")
        || html.contains("dmbox-disambig")
}

/// Extracts disambiguation target links grouped by section heading.
fn mediawiki_disambiguation_link_groups(
    language: &str,
    html: &str,
) -> Vec<DisambiguationLinkGroup> {
    let document = Html::parse_fragment(html);
    let selector =
        Selector::parse("h2, h3, h4, h5, h6, li").expect("disambiguation selector is valid");
    let link_selector = Selector::parse("a[href]").expect("link selector is valid");
    let mut current_group = "Other uses".to_string();
    let mut seen = HashSet::new();
    let mut groups = Vec::new();
    let mut link_count = 0;

    for element in document.select(&selector) {
        if should_skip_disambiguation_element(element) {
            continue;
        }

        match element.value().name() {
            "h2" | "h3" | "h4" | "h5" | "h6" => {
                if let Some(title) = disambiguation_heading_text(language, element) {
                    current_group = title;
                }
            }
            "li" => {
                let Some(link) = disambiguation_link_from_list_item(
                    language,
                    element,
                    &link_selector,
                    &mut seen,
                ) else {
                    continue;
                };

                push_disambiguation_link_group(&mut groups, &current_group, link);
                link_count += 1;

                if link_count >= DISAMBIGUATION_LINK_LIMIT {
                    break;
                }
            }
            _ => {}
        }
    }

    groups
}

/// Skips page chrome and helper boxes while walking disambiguation HTML.
fn should_skip_disambiguation_element(element: ElementRef<'_>) -> bool {
    element_or_ancestor_has_class_part(
        element,
        &[
            "metadata",
            "navbox",
            "sistersitebox",
            "selfreference",
            "reflist",
            "reference",
            "portal",
            "noprint",
            "toc",
        ],
    )
}

/// Extracts a clean section heading for disambiguation button groups.
fn disambiguation_heading_text(language: &str, heading: ElementRef<'_>) -> Option<String> {
    let rendered = trim_html_spacing(&render_inline_element(language, heading));
    let title = html_to_plain_text(&rendered);
    let title = title.split_whitespace().collect::<Vec<_>>().join(" ");
    (!title.is_empty() && !is_terminal_article_heading(&title)).then_some(title)
}

/// Extracts the first usable target link from a disambiguation list item.
fn disambiguation_link_from_list_item(
    language: &str,
    item: ElementRef<'_>,
    link_selector: &Selector,
    seen: &mut HashSet<String>,
) -> Option<NavLink> {
    let link = item.select(link_selector).find(|link| {
        !element_has_class_part(*link, &["selflink", "new", "mw-disambig"])
            && link
                .value()
                .attr("href")
                .and_then(wiki_article_title_from_href)
                .is_some()
    })?;

    let page_title = link
        .value()
        .attr("href")
        .and_then(wiki_article_title_from_href)?;

    let key = normalized_title_key(&page_title);
    if !seen.insert(key) {
        return None;
    }

    let label = trim_html_spacing(&render_inline_element(language, link));
    let label = html_to_plain_text(&label);
    let label = label.trim();
    Some(NavLink {
        label: if label.is_empty() {
            page_title.clone()
        } else {
            label.to_string()
        },
        page_title,
    })
}

/// Appends a disambiguation link to the current group or starts a new group.
fn push_disambiguation_link_group(
    groups: &mut Vec<DisambiguationLinkGroup>,
    title: &str,
    link: NavLink,
) {
    if let Some(group) = groups.last_mut()
        && group.title == title
    {
        group.links.push(link);
        return;
    }

    groups.push(DisambiguationLinkGroup {
        title: title.to_string(),
        links: vec![link],
    });
}

/// Extracts the first internal article-body links for a follow-up buttons
/// message.
fn mediawiki_body_links(language: &str, article_title: &str, html: &str) -> Vec<NavLink> {
    let document = Html::parse_fragment(html);
    let selector = Selector::parse("a[href]").expect("body link selector is valid");
    let article_key = normalized_title_key(article_title);
    let mut seen = HashSet::new();
    let mut links = Vec::new();

    for link in document.select(&selector) {
        if element_has_class_part(link, &["selflink", "new"])
            || element_or_ancestor_has_class_part(
                link,
                &[
                    "infobox",
                    "navbox",
                    "vertical-navbox",
                    "sidebar",
                    "metadata",
                    "sistersitebox",
                    "selfreference",
                    "reflist",
                    "reference",
                    "ambox",
                    "hatnote",
                    "noprint",
                    "gallery",
                    "portal",
                    "mw-editsection",
                    "mw-empty-elt",
                ],
            )
        {
            continue;
        }

        let Some(page_title) = link
            .value()
            .attr("href")
            .and_then(wiki_article_title_from_href)
        else {
            continue;
        };

        let key = normalized_title_key(&page_title);
        if key == article_key || !seen.insert(key) {
            continue;
        }

        let label = trim_html_spacing(&render_inline_element(language, link));
        let label = html_to_plain_text(&label);
        let label = label.trim();
        if label.is_empty() {
            continue;
        }

        links.push(NavLink {
            label: label.to_string(),
            page_title,
        });

        if links.len() >= ARTICLE_BODY_LINK_LIMIT {
            break;
        }
    }

    links
}

/// Extracts navbox/sidebar templates and their internal article links.
fn mediawiki_nav_templates(language: &str, html: &str) -> Vec<NavTemplate> {
    let document = Html::parse_fragment(html);
    let selector =
        Selector::parse(".navbox, .vertical-navbox, .sidebar").expect("nav selector is valid");
    let link_selector = Selector::parse("a[href]").expect("nav link selector is valid");
    let mut seen = HashSet::new();
    let mut templates = Vec::new();

    for template in document.select(&selector) {
        let mut links = Vec::new();
        for link in template.select(&link_selector) {
            if element_has_class_part(link, &["selflink", "new"]) {
                continue;
            }

            let Some(page_title) = link
                .value()
                .attr("href")
                .and_then(wiki_article_title_from_href)
            else {
                continue;
            };

            let key = normalized_title_key(&page_title);
            if !seen.insert(key) {
                continue;
            }

            let label = trim_html_spacing(&render_inline_element(language, link));
            let label = html_to_plain_text(&label);
            let label = label.trim();
            if label.is_empty() {
                continue;
            }

            links.push(NavLink {
                label: label.to_string(),
                page_title,
            });

            if links.len() >= NAV_TEMPLATE_LINK_LIMIT {
                break;
            }
        }

        if !links.is_empty() {
            let title = nav_template_title(language, template);
            templates.push(NavTemplate {
                title: title
                    .as_ref()
                    .map(|title| title.text.clone())
                    .unwrap_or_else(|| "Navigation".to_string()),
                title_url: title.and_then(|title| title.url),
                links,
            });
        }
    }

    templates
}

/// Extracts the displayed title for a navigation template.
fn nav_template_title(language: &str, template: ElementRef<'_>) -> Option<NavTemplateTitle> {
    let title_selector = Selector::parse(
        ".navbox-title, th.navbox-title, .sidebar-title, .sidebar-heading, .vertical-navbox-title",
    )
    .expect("nav title selector is valid");

    template.select(&title_selector).find_map(|element| {
        let rendered = trim_html_spacing(&render_inline_element(language, element));
        let title = html_to_plain_text(&rendered);
        let title = title
            .split_whitespace()
            .filter(|part| !matches!(*part, "v" | "t" | "e"))
            .collect::<Vec<_>>()
            .join(" ");
        (!title.is_empty()).then(|| NavTemplateTitle {
            url: nav_template_title_url(language, element),
            text: title,
        })
    })
}

#[derive(Debug, Clone)]
struct NavTemplateTitle {
    text: String,
    url: Option<String>,
}

/// Finds a usable link target for a navigation template title.
fn nav_template_title_url(language: &str, title_element: ElementRef<'_>) -> Option<String> {
    let link_selector = Selector::parse("a[href]").expect("nav title link selector is valid");
    title_element.select(&link_selector).find_map(|link| {
        if element_or_ancestor_has_class_part(link, &["navbar"])
            || element_has_class_part(link, &["new", "selflink"])
        {
            return None;
        }
        let label = html_to_plain_text(&render_inline_element(language, link));
        if matches!(label.trim(), "v" | "t" | "e") {
            return None;
        }
        link.value()
            .attr("href")
            .and_then(|href| normalize_wiki_href(language, href))
    })
}

/// Converts an internal `/wiki/...` href into a namespace-free article title.
fn wiki_article_title_from_href(href: &str) -> Option<String> {
    let path = href.trim().strip_prefix("/wiki/")?;
    if path.is_empty() || path.contains('#') {
        return None;
    }

    let decoded = urlencoding::decode(path).ok()?;
    let title = decoded.replace('_', " ");
    if title.contains(':') {
        return None;
    }

    Some(title)
}

/// Normalizes titles for deduplication and lookup map keys.
fn normalized_title_key(title: &str) -> String {
    title
        .replace('_', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// Checks MediaWiki pageprops for the disambiguation marker.
fn info_page_is_disambiguation(page: &InfoPage) -> bool {
    page.pageprops
        .as_ref()
        .is_some_and(|props| props.contains_key("disambiguation"))
}

/// Renders one reference list item with visible links.
fn render_reference_item(language: &str, item: ElementRef<'_>) -> Option<String> {
    let mut text = String::new();
    let mut links = Vec::new();
    for child in item.children() {
        render_reference_node(language, child, &mut text, &mut links);
    }

    let text = trim_html_spacing(&text);
    let mut lines = Vec::new();
    if !text.is_empty() {
        lines.push(text);
    }
    lines.extend(links.into_iter().map(|link| html_link(&link, &link)));

    (!lines.is_empty()).then(|| lines.join("\n"))
}

/// Recursively renders reference content and collects links found in it.
fn render_reference_node(
    language: &str,
    node: NodeRef<'_, Node>,
    out: &mut String,
    links: &mut Vec<String>,
) {
    match node.value() {
        Node::Text(text) => append_collapsed_text(out, text.as_ref()),
        Node::Element(_) => {
            let Some(element) = ElementRef::wrap(node) else {
                return;
            };
            if should_skip_reference_inline(element) {
                return;
            }

            if element.value().name() == "a"
                && let Some(url) = element
                    .value()
                    .attr("href")
                    .and_then(|href| normalize_wiki_href(language, href))
            {
                push_unique_link(links, url);
            }

            if element.value().name() == "br" {
                out.push('\n');
                return;
            }

            for child in element.children() {
                render_reference_node(language, child, out, links);
            }
        }
        _ => {}
    }
}

/// Skips inline elements that are not useful in reference output.
fn should_skip_reference_inline(element: ElementRef<'_>) -> bool {
    let name = element.value().name();
    matches!(
        name,
        "style" | "script" | "noscript" | "img" | "audio" | "video"
    ) || element_has_class_part(
        element,
        &["mw-cite-backlink", "mw-editsection", "mw-empty-elt"],
    )
}

/// Pushes a link if it has not already been collected.
fn push_unique_link(links: &mut Vec<String>, url: String) {
    if !links.iter().any(|link| link == &url) {
        links.push(url);
    }
}

/// Walks block-level MediaWiki HTML and appends Telegram-renderable blocks to
/// article sections.
fn collect_html_blocks(
    language: &str,
    element: ElementRef<'_>,
    sections: &mut ArticleBodySectionBuilder,
) {
    if element.value().name() == "img" {
        if let Some(image) = media_item_from_html_image(language, element) {
            sections.push_image(image);
        }
        return;
    }

    if element.value().name() == "figure" {
        collect_html_images(language, element, sections);
        return;
    }

    if should_skip_block(element) {
        return;
    }

    let name = element.value().name();
    match name {
        "p" => {
            let rendered = trim_html_spacing(&render_inline_element(language, element));
            if !rendered.is_empty() {
                sections.push_block(rendered);
            }
        }
        "blockquote" => {
            let rendered = trim_html_spacing(&render_inline_element(language, element));
            if !rendered.is_empty() {
                sections.push_block(format!("<blockquote>{rendered}</blockquote>"));
            }
        }
        "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
            let rendered = trim_html_spacing(&render_inline_element(language, element));
            let heading_text = html_to_plain_text(&rendered);
            if !rendered.is_empty() && !is_terminal_article_heading(&heading_text) {
                sections.start_section(ArticleBodyHeading {
                    title: heading_text,
                    anchor: element_anchor_id(element),
                });
            }
        }
        "ul" => {
            let rendered = render_list(language, element, false);
            if !rendered.is_empty() {
                sections.push_block(rendered);
            }
        }
        "ol" => {
            let rendered = render_list(language, element, true);
            if !rendered.is_empty() {
                sections.push_block(rendered);
            }
        }
        "table" => {
            let rendered = render_table(language, element);
            if !rendered.is_empty() {
                sections.push_block(rendered);
            }
        }
        _ => {
            for child in element.children() {
                if let Some(child_element) = ElementRef::wrap(child) {
                    collect_html_blocks(language, child_element, sections);
                }
            }
        }
    }
}

/// Collects image tags from a figure into the current article section.
fn collect_html_images(
    language: &str,
    element: ElementRef<'_>,
    sections: &mut ArticleBodySectionBuilder,
) {
    let image_selector = Selector::parse("img[src]").expect("image selector is valid");
    for image in element.select(&image_selector) {
        if let Some(image) = media_item_from_html_image(language, image) {
            sections.push_image(image);
        }
    }
}

/// Converts an HTML image element into a media item when it is likely article
/// content.
fn media_item_from_html_image(language: &str, image: ElementRef<'_>) -> Option<MediaItem> {
    if element_or_ancestor_has_class_part(
        image,
        &[
            "infobox",
            "navbox",
            "metadata",
            "reflist",
            "reference",
            "gallery",
            "mw-kartographer",
            "locmap",
        ],
    ) {
        return None;
    }

    let alt = image.value().attr("alt").unwrap_or_default().trim();
    if alt.eq_ignore_ascii_case("map") {
        return None;
    }

    let src = image.value().attr("src").and_then(normalize_image_src)?;
    if !looks_like_image_url(&src) {
        return None;
    }
    let original = wikimedia_original_image_url_from_thumbnail(&src).unwrap_or_else(|| src.clone());
    let fallback_url = (original != src)
        .then(|| wikimedia_thumbnail_url_with_width(&src, ARTICLE_IMAGE_FALLBACK_THUMB_WIDTH))
        .flatten()
        .or_else(|| (original != src).then_some(src));

    let title = image
        .ancestors()
        .filter_map(ElementRef::wrap)
        .find(|element| element.value().name() == "a")
        .and_then(|link| link.value().attr("title"))
        .or_else(|| (!alt.is_empty()).then_some(alt))
        .unwrap_or("Article image")
        .to_string();
    let item = MediaItem {
        title,
        url: original,
        fallback_url,
        caption: image_caption_from_ancestor(language, image),
        mime: None,
        width: image.value().attr("width").and_then(parse_u64_attr),
        height: image.value().attr("height").and_then(parse_u64_attr),
    };

    is_gallery_image(&item).then_some(item)
}

/// Parses a positive integer HTML attribute.
fn parse_u64_attr(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

/// Finds an image caption near the image element.
fn image_caption_from_ancestor(language: &str, image: ElementRef<'_>) -> Option<String> {
    let caption_selector = Selector::parse("figcaption").expect("figcaption selector is valid");
    image
        .ancestors()
        .filter_map(ElementRef::wrap)
        .find(|element| element.value().name() == "figure")
        .and_then(|figure| figure.select(&caption_selector).next())
        .map(|caption| trim_html_spacing(&render_inline_element(language, caption)))
        .filter(|caption| !caption.is_empty())
}

/// Extracts the MediaWiki anchor ID attached to a heading.
fn element_anchor_id(element: ElementRef<'_>) -> Option<String> {
    if let Some(id) = element.value().attr("id").filter(|id| !id.is_empty()) {
        return Some(id.to_string());
    }

    for child in element.children() {
        let Some(child_element) = ElementRef::wrap(child) else {
            continue;
        };
        if element_has_class_part(child_element, &["mw-headline"])
            && let Some(id) = child_element.value().attr("id").filter(|id| !id.is_empty())
        {
            return Some(id.to_string());
        }
        if let Some(id) = element_anchor_id(child_element) {
            return Some(id);
        }
    }

    None
}

/// Renders ordered or unordered lists into simple Telegram HTML lines.
fn render_list(language: &str, element: ElementRef<'_>, ordered: bool) -> String {
    let mut lines = Vec::new();
    let mut index = 1usize;

    for child in element.children() {
        let Some(item) = ElementRef::wrap(child) else {
            continue;
        };
        if item.value().name() != "li" || should_skip_block(item) {
            continue;
        }

        let rendered = trim_html_spacing(&render_inline_element(language, item));
        if rendered.is_empty() {
            continue;
        }

        if ordered {
            lines.push(format!("{index}. {rendered}"));
            index += 1;
        } else {
            lines.push(format!("- {rendered}"));
        }
    }

    lines.join("\n")
}

/// Renders a MediaWiki table as a monospace text table plus visible links.
fn render_table(language: &str, table: ElementRef<'_>) -> String {
    let mut rows = Vec::new();
    collect_table_rows(table, &mut rows);
    let mut links = Vec::new();

    let rows = rows
        .into_iter()
        .filter_map(|row| render_table_row(language, row, &mut links))
        .collect::<Vec<_>>();

    let table = format_plain_text_table(&rows);
    let links = format_table_links(&links);
    [table, links]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Recursively collects table rows from nested table sections.
fn collect_table_rows<'a>(element: ElementRef<'a>, rows: &mut Vec<ElementRef<'a>>) {
    for child in element.children() {
        let Some(child_element) = ElementRef::wrap(child) else {
            continue;
        };
        match child_element.value().name() {
            "tr" => rows.push(child_element),
            "table" => {}
            _ => collect_table_rows(child_element, rows),
        }
    }
}

/// Renders one table row into plain text cells.
fn render_table_row(
    language: &str,
    row: ElementRef<'_>,
    links: &mut Vec<TableLink>,
) -> Option<Vec<String>> {
    let cells = row
        .children()
        .filter_map(ElementRef::wrap)
        .filter(|cell| matches!(cell.value().name(), "th" | "td"))
        .flat_map(|cell| {
            collect_table_links(language, cell, links);
            let colspan = cell_colspan(cell);
            let rendered = trim_html_spacing(&render_inline_element(language, cell));
            let plain = html_to_plain_text(&rendered)
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .collect::<Vec<_>>()
                .join(" / ");
            if plain.is_empty() {
                return Vec::new();
            }

            let mut expanded = Vec::with_capacity(colspan);
            expanded.push(truncate_for_telegram(&plain, TABLE_CELL_MAX_CHARS));
            expanded.extend((1..colspan).map(|_| String::new()));
            expanded
        })
        .collect::<Vec<_>>();

    (!cells.is_empty()).then_some(cells)
}

/// Reads a table cell colspan, defaulting to one column.
fn cell_colspan(cell: ElementRef<'_>) -> usize {
    cell.value()
        .attr("colspan")
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(12))
        .unwrap_or(1)
}

/// Aligns plain text table rows for a Telegram `<pre>` block.
fn format_plain_text_table(rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let column_count = rows.iter().map(Vec::len).max().unwrap_or(0);
    if column_count == 0 {
        return String::new();
    }

    let mut widths = vec![0usize; column_count];
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.chars().count());
        }
    }

    let table = rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(index, cell)| pad_right_chars(cell, widths[index]))
                .collect::<Vec<_>>()
                .join(" | ")
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "{TELEGRAM_PRE_OPEN}{}{TELEGRAM_PRE_CLOSE}",
        html_escape_text(&table)
    )
}

/// Pads a string by visible character count.
fn pad_right_chars(value: &str, width: usize) -> String {
    let count = value.chars().count();
    if count >= width {
        return value.to_string();
    }

    let mut padded = String::with_capacity(value.len() + width.saturating_sub(count));
    padded.push_str(value);
    padded.extend(std::iter::repeat_n(' ', width - count));
    padded
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableLink {
    text: String,
    url: String,
}

/// Collects links that were present inside a rendered table.
fn collect_table_links(language: &str, element: ElementRef<'_>, links: &mut Vec<TableLink>) {
    if element.value().name() == "a"
        && let Some(url) = element
            .value()
            .attr("href")
            .and_then(|href| normalize_wiki_href(language, href))
    {
        let text = html_to_plain_text(&render_inline_element(language, element));
        let text = if text.is_empty() { url.clone() } else { text };
        if !links.iter().any(|link| link.url == url) {
            links.push(TableLink { text, url });
        }
    }

    for child in element.children() {
        if let Some(child_element) = ElementRef::wrap(child) {
            collect_table_links(language, child_element, links);
        }
    }
}

/// Formats collected table links under the table block.
fn format_table_links(links: &[TableLink]) -> String {
    if links.is_empty() {
        return String::new();
    }

    let links = links
        .iter()
        .map(|link| html_link(&link.url, &link.text))
        .collect::<Vec<_>>()
        .join("\n");
    format!("{}\n{}", html_bold("Table links"), links)
}

/// Renders an element's inline children to Telegram-compatible HTML.
fn render_inline_element(language: &str, element: ElementRef<'_>) -> String {
    let mut out = String::new();
    render_inline_children(language, element, &mut out);
    trim_html_spacing(&out)
}

/// Renders inline child nodes into an existing output buffer.
fn render_inline_children(language: &str, element: ElementRef<'_>, out: &mut String) {
    for child in element.children() {
        render_inline_node(language, child, out);
    }
}

/// Renders one inline HTML node, preserving Telegram-supported formatting.
fn render_inline_node(language: &str, node: NodeRef<'_, Node>, out: &mut String) {
    match node.value() {
        Node::Text(text) => append_collapsed_text(out, text.as_ref()),
        Node::Element(_) => {
            let Some(element) = ElementRef::wrap(node) else {
                return;
            };
            if should_skip_inline(element) {
                return;
            }

            let name = element.value().name();
            match name {
                "a" => {
                    let inner = render_inline_element(language, element);
                    if inner.is_empty() {
                        return;
                    }

                    if let Some(url) = element
                        .value()
                        .attr("href")
                        .and_then(|href| normalize_wiki_href(language, href))
                    {
                        out.push_str(&format!(
                            "<a href=\"{}\">{inner}</a>",
                            html_escape::encode_double_quoted_attribute(&url)
                        ));
                    } else {
                        out.push_str(&inner);
                    }
                }
                "b" | "strong" => render_inline_wrapped(language, element, out, "b"),
                "i" | "em" => render_inline_wrapped(language, element, out, "i"),
                "code" | "kbd" => render_inline_wrapped(language, element, out, "code"),
                "br" => out.push('\n'),
                "p" | "div" | "span" | "small" | "abbr" | "cite" | "q" => {
                    render_inline_children(language, element, out);
                }
                _ => render_inline_children(language, element, out),
            }
        }
        _ => {}
    }
}

/// Renders a supported inline wrapper tag if it contains non-empty text.
fn render_inline_wrapped(language: &str, element: ElementRef<'_>, out: &mut String, tag: &str) {
    let inner = render_inline_element(language, element);
    if !inner.is_empty() {
        out.push_str(&format!("<{tag}>{inner}</{tag}>"));
    }
}

/// Skips block elements that are page chrome, references, or unsupported media.
fn should_skip_block(element: ElementRef<'_>) -> bool {
    let name = element.value().name();
    matches!(
        name,
        "style" | "script" | "noscript" | "figure" | "img" | "audio" | "video"
    ) || (name == "table" && is_climate_data_table(element))
        || element_has_class_part(
            element,
            &[
                "infobox",
                "navbox",
                "metadata",
                "reflist",
                "reference",
                "ambox",
                "hatnote",
                "noprint",
                "gallery",
                "portal",
                "mw-editsection",
                "mw-empty-elt",
            ],
        )
}

/// Detects climate/weather tables that are too wide to be useful in chat.
fn is_climate_data_table(table: ElementRef<'_>) -> bool {
    if element_has_class_part(table, &["weatherbox", "climate"]) {
        return true;
    }

    let text = table
        .text()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = text.to_lowercase();

    (lower.starts_with("climate data for ") || lower.contains(" climate data for "))
        && lower.contains("month")
        && (lower.contains("record high") || lower.contains("daily mean"))
}

/// Skips inline elements that Telegram should not receive.
fn should_skip_inline(element: ElementRef<'_>) -> bool {
    let name = element.value().name();
    matches!(
        name,
        "sup" | "style" | "script" | "noscript" | "img" | "audio" | "video"
    ) || element_has_class_part(
        element,
        &[
            "mw-cite-backlink",
            "mw-editsection",
            "mw-empty-elt",
            "error",
        ],
    )
}

/// Checks whether an element's class list contains any configured substring.
fn element_has_class_part(element: ElementRef<'_>, needles: &[&str]) -> bool {
    element.value().attr("class").is_some_and(|classes| {
        classes.split_whitespace().any(|class| {
            let class = class.to_ascii_lowercase();
            needles.iter().any(|needle| class.contains(needle))
        })
    })
}

/// Checks an element and its ancestors for class substrings.
fn element_or_ancestor_has_class_part(element: ElementRef<'_>, needles: &[&str]) -> bool {
    element_has_class_part(element, needles)
        || element
            .ancestors()
            .filter_map(ElementRef::wrap)
            .any(|ancestor| element_has_class_part(ancestor, needles))
}

/// Converts internal and protocol-relative wiki hrefs into absolute URLs.
fn normalize_wiki_href(language: &str, href: &str) -> Option<String> {
    let href = href.trim();
    if href.is_empty()
        || href.starts_with('#')
        || href.starts_with("javascript:")
        || href.contains("redlink=1")
    {
        return None;
    }

    if href.starts_with("https://") || href.starts_with("http://") {
        return Some(href.to_string());
    }

    if let Some(rest) = href.strip_prefix("//") {
        return Some(format!("https://{rest}"));
    }

    if let Some(path) = href.strip_prefix("/wiki/") {
        return Some(format!("https://{language}.wikipedia.org/wiki/{path}"));
    }

    if let Some(path) = href.strip_prefix("./") {
        return Some(format!("https://{language}.wikipedia.org/wiki/{path}"));
    }

    None
}

/// Appends text while collapsing whitespace to Telegram-friendly spacing.
fn append_collapsed_text(out: &mut String, text: &str) {
    let has_leading_space = text.chars().next().is_some_and(char::is_whitespace);
    let has_trailing_space = text.chars().last().is_some_and(char::is_whitespace);
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");

    if collapsed.is_empty() {
        if has_leading_space || has_trailing_space {
            append_single_space(out);
        }
        return;
    }

    if has_leading_space {
        append_single_space(out);
    }
    out.push_str(&html_escape_text(&collapsed));
    if has_trailing_space {
        append_single_space(out);
    }
}

/// Appends at most one space unless the output already ends at a boundary.
fn append_single_space(out: &mut String) {
    if !out.is_empty() && !out.ends_with(' ') && !out.ends_with('\n') {
        out.push(' ');
    }
}

/// Normalizes spacing around punctuation and newlines after HTML rendering.
fn trim_html_spacing(value: &str) -> String {
    value
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

/// Converts Telegram HTML fragments back to plain text.
fn html_to_plain_text(value: &str) -> String {
    html_to_text(value)
}

/// Identifies terminal article sections that should not start body output.
fn is_terminal_article_heading(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "references" | "notes" | "external links" | "further reading" | "bibliography" | "sources"
    )
}

/// Chooses a shorter infobox cap for very large articles.
fn article_infobox_limit(article_length: Option<usize>) -> usize {
    if article_length.is_some_and(|length| length > 50_000) {
        1400
    } else {
        1800
    }
}

/// Renders article metadata as Telegram HTML.
fn render_metadata_html(metadata: &ArticleMetadata) -> String {
    let mut lines = Vec::new();
    append_metadata_summary_lines(&mut lines, metadata);
    append_metadata_pageview_lines(&mut lines, metadata);
    append_metadata_language_lines(&mut lines, &metadata.language_links);
    append_metadata_external_link_lines(&mut lines, &metadata.external_links);
    append_article_revision_lines(&mut lines, &metadata.language, &metadata.revisions);
    lines.join("\n")
}

/// Appends identity, URL, Wikidata, Commons, and coordinate fields.
fn append_metadata_summary_lines(lines: &mut Vec<String>, metadata: &ArticleMetadata) {
    let info = &metadata.info;
    lines.push(html_bold(&format!("Metadata for {}", info.title)));
    lines.push(format!(
        "Language: {}",
        html_escape_text(&metadata.language)
    ));
    if let Some(page_id) = info.page_id {
        lines.push(format!("Page ID: {page_id}"));
    }
    if let Some(length) = info.length {
        lines.push(format!("Length: {length} bytes"));
    }
    if let Some(last_revision_id) = info.last_revision_id {
        lines.push(format!(
            "Latest revision: {}",
            revision_link(&metadata.language, last_revision_id)
        ));
    }
    if let Some(touched) = &info.touched {
        lines.push(format!("Touched: {}", html_escape_text(touched)));
    }
    if let Some(full_url) = &info.full_url {
        lines.push(format!("URL: {}", html_link(full_url, full_url)));
    }
    if let Some(wikidata) = info.page_props.get("wikibase_item") {
        lines.push(format!(
            "Wikidata: {}",
            html_link(
                &format!("https://www.wikidata.org/wiki/{wikidata}"),
                wikidata
            )
        ));
    }
    if let Some(commons) = &metadata.commons_url {
        lines.push(format!(
            "Wikimedia Commons: {}",
            html_link(commons, commons)
        ));
    }
    if let Some(disambiguation) = info.page_props.get("disambiguation") {
        lines.push(format!(
            "Disambiguation: {}",
            html_escape_text(disambiguation)
        ));
    }
    if let Some(coordinate) = info.coordinates.first() {
        lines.push(metadata_coordinate_line(coordinate));
    }
}

/// Formats the first coordinate as a clickable map link.
fn metadata_coordinate_line(coordinate: &Coordinate) -> String {
    let globe = coordinate
        .globe
        .as_ref()
        .map(|globe| format!(" ({})", html_escape_text(globe)))
        .unwrap_or_default();
    format!(
        "Coordinates: {}{}",
        html_link(
            &coordinate_url(coordinate.lat, coordinate.lon),
            &format!("{}, {}", coordinate.lat, coordinate.lon)
        ),
        globe
    )
}

/// Appends pageview totals and the latest daily value.
fn append_metadata_pageview_lines(lines: &mut Vec<String>, metadata: &ArticleMetadata) {
    if metadata.pageviews.days > 0 {
        lines.push(format!(
            "{}: {}",
            html_link(
                &pageviews_url(&metadata.language, &metadata.info.title),
                &format!("Views last {} days", metadata.pageviews.days)
            ),
            metadata.pageviews.total
        ));
        if let Some((date, views)) = &metadata.pageviews.latest {
            lines.push(format!(
                "Latest daily views: {} on {}",
                views,
                html_escape_text(date)
            ));
        }
    }
}

/// Appends language-link counts and a compact language preview.
fn append_metadata_language_lines(lines: &mut Vec<String>, language_links: &[LanguageLink]) {
    lines.push(format!("Language links: {}", language_links.len()));
    if !language_links.is_empty() {
        lines.push(format!(
            "Languages: {}",
            language_links
                .iter()
                .take(12)
                .map(metadata_language_link_label)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
}

/// Formats one language-link label.
fn metadata_language_link_label(link: &LanguageLink) -> String {
    let label = link
        .langname
        .as_deref()
        .map(|name| format!("{} ({})", link.lang, name))
        .unwrap_or_else(|| link.lang.clone());
    html_escape_text(&label)
}

/// Appends external-link counts and the first few clickable URLs.
fn append_metadata_external_link_lines(lines: &mut Vec<String>, external_links: &[String]) {
    lines.push(format!("External links: {}", external_links.len()));
    for link in external_links.iter().take(8) {
        lines.push(format!("External: {}", html_link(link, link)));
    }
}

/// Appends recent article revisions.
fn append_article_revision_lines(lines: &mut Vec<String>, language: &str, revisions: &[Revision]) {
    if !revisions.is_empty() {
        lines.push(html_bold("Last edits:"));
        for revision in revisions {
            lines.push(article_revision_line(language, revision));
        }
    }
}

/// Formats one article revision row.
fn article_revision_line(language: &str, revision: &Revision) -> String {
    let comment = revision
        .comment
        .as_deref()
        .map(str::trim)
        .filter(|comment| !comment.is_empty())
        .unwrap_or("(no edit summary)");
    let tags = revision_tags_suffix(revision);
    let revision_text = revision
        .revid
        .map(|revid| revision_link(language, revid))
        .unwrap_or_else(|| "rev 0".to_string());
    format!(
        "- {} | {} | {} | size {} | {}{}",
        html_escape_text(revision.timestamp.as_deref().unwrap_or("unknown time")),
        revision_user_link(language, revision.user.as_deref()),
        revision_text,
        revision
            .size
            .map(|size| size.to_string())
            .unwrap_or_else(|| "?".to_string()),
        html_escape_text(&truncate_for_telegram(comment, 240)),
        tags
    )
}

/// Formats revision tags when Wikimedia returns any.
fn revision_tags_suffix(revision: &Revision) -> String {
    revision
        .tags
        .as_ref()
        .filter(|tags| !tags.is_empty())
        .map(|tags| format!(" [{}]", html_escape_text(&tags.join(", "))))
        .unwrap_or_default()
}

/// Renders Wikidata claims into Telegram-sized HTML messages.
fn render_wikidata_claims_html_parts(claims: &WikidataClaims, limit: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let title = claims
        .label
        .as_ref()
        .map(|label| {
            if label == &claims.item {
                claims.item.clone()
            } else {
                format!("{label} ({})", claims.item)
            }
        })
        .unwrap_or_else(|| claims.item.clone());
    lines.push(html_bold_link(&wikidata_entity_url(&claims.item), &title));
    if let Some(description) = claims.description.as_deref() {
        lines.push(html_escape_text(description));
    }
    lines.push(format!("Properties: {}", claims.property_count));

    for property in claims
        .properties
        .iter()
        .take(WIKIDATA_PROPERTY_DISPLAY_LIMIT)
    {
        let property_title = if property.property_label == property.property_id {
            property.property_id.clone()
        } else {
            format!("{} ({})", property.property_label, property.property_id)
        };
        let property_link = html_link(&wikidata_entity_url(&property.property_id), &property_title);
        let mut values = if property.values.is_empty() {
            vec![html_escape_text("no value")]
        } else {
            property.values.clone()
        };
        if property.total_values > values.len() {
            values.push(format!("+{} more", property.total_values - values.len()));
        }
        lines.push(format!("{property_link}: {}", values.join(", ")));
    }

    if claims.properties.len() > WIKIDATA_PROPERTY_DISPLAY_LIMIT {
        lines.push(format!(
            "Shown first {} properties.",
            WIKIDATA_PROPERTY_DISPLAY_LIMIT
        ));
    }

    split_html_lines_for_telegram(&lines.join("\n"), limit)
}

/// Renders metadata for the Wikidata entity page itself.
fn render_wikidata_page_metadata_html(metadata: &WikidataPageMetadata) -> String {
    let mut lines = Vec::new();
    lines.push(html_bold(&format!(
        "Wikidata metadata for {}",
        metadata.item
    )));
    lines.push(format!(
        "Info page: {}",
        html_link(&wikidata_action_info_url(&metadata.item), "action=info")
    ));
    if let Some(page_id) = metadata.page_id {
        lines.push(format!("Page ID: {page_id}"));
    }
    lines.push(format!("Title: {}", html_escape_text(&metadata.title)));
    if let Some(length) = metadata.length {
        lines.push(format!("Length: {length} bytes"));
    }
    if let Some(last_revision_id) = metadata.last_revision_id {
        lines.push(format!(
            "Latest revision: {}",
            wikidata_revision_link(last_revision_id)
        ));
    }
    if let Some(touched) = &metadata.touched {
        lines.push(format!("Touched: {}", html_escape_text(touched)));
    }
    if let Some(full_url) = &metadata.full_url {
        lines.push(format!("URL: {}", html_link(full_url, full_url)));
    }

    if !metadata.revisions.is_empty() {
        lines.push(html_bold("Last edits:"));
        for revision in &metadata.revisions {
            let comment = revision
                .comment
                .as_deref()
                .map(str::trim)
                .filter(|comment| !comment.is_empty())
                .unwrap_or("(no edit summary)");
            let tags = revision
                .tags
                .as_ref()
                .filter(|tags| !tags.is_empty())
                .map(|tags| format!(" [{}]", html_escape_text(&tags.join(", "))))
                .unwrap_or_default();
            let revision_text = revision
                .revid
                .map(wikidata_revision_link)
                .unwrap_or_else(|| "rev 0".to_string());
            lines.push(format!(
                "- {} | {} | {} | size {} | {}{}",
                html_escape_text(revision.timestamp.as_deref().unwrap_or("unknown time")),
                wikidata_user_link(revision.user.as_deref()),
                revision_text,
                revision
                    .size
                    .map(|size| size.to_string())
                    .unwrap_or_else(|| "?".to_string()),
                html_escape_text(&truncate_for_telegram(comment, 240)),
                tags
            ));
        }
    }

    lines.join("\n")
}

/// Converts one Wikidata snak value into clickable user-facing text when
/// possible.
fn render_wikidata_snak_value(
    property_id: &str,
    snak: &WikidataSnak,
    labels: &HashMap<String, WikidataLabelInfo>,
    property_formatters: &HashMap<String, String>,
) -> Option<String> {
    let Some(datavalue) = &snak.datavalue else {
        return render_wikidata_missing_snak_value(snak);
    };

    if let Some(entity_id) = wikidata_entity_id_from_value(&datavalue.value) {
        return Some(wikidata_entity_link(&entity_id, labels));
    }

    if let Some(value) = datavalue.value.as_str() {
        return Some(render_wikidata_string_value(
            property_id,
            snak.datatype.as_deref(),
            value,
            property_formatters,
        ));
    }

    if let Some(value) = datavalue.value.as_object()
        && let Some(rendered) = render_wikidata_object_value(value, labels)
    {
        return Some(rendered);
    }

    Some(render_unknown_wikidata_value(&datavalue.value))
}

/// Renders Wikidata snaks without a concrete datavalue.
fn render_wikidata_missing_snak_value(snak: &WikidataSnak) -> Option<String> {
    match snak.snaktype.as_deref() {
        Some("somevalue") => Some(html_escape_text("some value")),
        Some("novalue") => Some(html_escape_text("no value")),
        _ => None,
    }
}

/// Renders string-shaped Wikidata values.
fn render_wikidata_string_value(
    property_id: &str,
    datatype: Option<&str>,
    value: &str,
    property_formatters: &HashMap<String, String>,
) -> String {
    if datatype == Some("external-id")
        && let Some(formatter) = property_formatters.get(property_id)
        && let Some(url) = wikidata_external_id_url(formatter, value)
    {
        return html_link(&url, value);
    }
    if datatype == Some("commonsMedia") {
        return html_link(&commons_file_url(value), value);
    }
    if is_absolute_url(value) {
        html_link(value, value)
    } else {
        html_escape_text(value)
    }
}

/// Renders object-shaped Wikidata values.
fn render_wikidata_object_value(
    value: &serde_json::Map<String, Value>,
    labels: &HashMap<String, WikidataLabelInfo>,
) -> Option<String> {
    wikidata_coordinate_value(value)
        .or_else(|| wikidata_time_value(value))
        .or_else(|| wikidata_quantity_value(value, labels))
        .or_else(|| wikidata_monolingual_text_value(value))
}

/// Renders any Wikidata value shape the bot does not understand yet.
fn render_unknown_wikidata_value(value: &Value) -> String {
    html_escape_text(&truncate_for_telegram(&value.to_string(), 240))
}

/// Formats a Wikidata globe-coordinate value as a clickable map link.
fn wikidata_coordinate_value(value: &serde_json::Map<String, Value>) -> Option<String> {
    let lat = value.get("latitude")?.as_f64()?;
    let lon = value.get("longitude")?.as_f64()?;
    Some(html_link(
        &coordinate_url(lat, lon),
        &format!("{lat}, {lon}"),
    ))
}

/// Formats a Wikidata time value, preserving simple precision when possible.
fn wikidata_time_value(value: &serde_json::Map<String, Value>) -> Option<String> {
    let raw = value.get("time")?.as_str()?;
    let mut formatted = raw.trim_start_matches('+').to_string();
    if let Some((date, _time)) = formatted.split_once('T') {
        formatted = date.to_string();
    }
    while formatted.ends_with("-00") {
        formatted.truncate(formatted.len().saturating_sub(3));
    }
    (!formatted.is_empty()).then(|| html_escape_text(&formatted))
}

/// Formats a Wikidata quantity value and optional unit entity.
fn wikidata_quantity_value(
    value: &serde_json::Map<String, Value>,
    labels: &HashMap<String, WikidataLabelInfo>,
) -> Option<String> {
    let amount = value.get("amount")?.as_str()?.trim_start_matches('+');
    let Some(unit) = value.get("unit").and_then(Value::as_str) else {
        return Some(html_escape_text(amount));
    };
    if unit == "1" {
        return Some(html_escape_text(amount));
    }

    let unit = wikidata_entity_id_from_url(unit)
        .map(|id| wikidata_entity_link(&id, labels))
        .unwrap_or_else(|| html_link(unit, unit));
    Some(format!("{} {}", html_escape_text(amount), unit))
}

/// Formats a Wikidata monolingual text value.
fn wikidata_monolingual_text_value(value: &serde_json::Map<String, Value>) -> Option<String> {
    let text = value.get("text")?.as_str()?;
    let language = value.get("language").and_then(Value::as_str);
    Some(match language {
        Some(language) => format!(
            "{} ({})",
            html_escape_text(text),
            html_escape_text(language)
        ),
        None => html_escape_text(text),
    })
}

/// Expands a Wikidata external-id formatter URL.
fn wikidata_external_id_url(formatter: &str, value: &str) -> Option<String> {
    let formatter = formatter.trim();
    let value = value.trim();
    if formatter.is_empty() || value.is_empty() || !formatter.contains("$1") {
        return None;
    }

    let url = formatter.replace("$1", &urlencoding::encode(value));
    is_absolute_url(&url).then_some(url)
}

/// Renders a Wikidata entity ID as a clickable label.
fn wikidata_entity_link(entity_id: &str, labels: &HashMap<String, WikidataLabelInfo>) -> String {
    let text = wikidata_label_text(labels, entity_id)
        .filter(|label| label != entity_id)
        .map(|label| format!("{label} ({entity_id})"))
        .unwrap_or_else(|| entity_id.to_string());
    html_link(&wikidata_entity_url(entity_id), &text)
}

/// Returns the best known label for a Wikidata entity.
fn wikidata_label_text(
    labels: &HashMap<String, WikidataLabelInfo>,
    entity_id: &str,
) -> Option<String> {
    labels.get(entity_id).and_then(|label| label.label.clone())
}

/// Returns the best known description for a Wikidata entity.
fn wikidata_description_text(
    labels: &HashMap<String, WikidataLabelInfo>,
    entity_id: &str,
) -> Option<String> {
    labels
        .get(entity_id)
        .and_then(|label| label.description.clone())
}

/// Extracts an entity ID from a Wikidata snak.
fn wikidata_entity_id_from_snak(snak: &WikidataSnak) -> Option<String> {
    snak.datavalue
        .as_ref()
        .and_then(|datavalue| wikidata_entity_id_from_value(&datavalue.value))
}

/// Extracts an entity ID from a Wikidata entity-id value object.
fn wikidata_entity_id_from_value(value: &Value) -> Option<String> {
    if let Some(id) = value
        .get("id")
        .and_then(Value::as_str)
        .and_then(normalize_wikidata_entity_id)
    {
        return Some(id);
    }

    let numeric_id = value.get("numeric-id")?.as_u64()?;
    match value.get("entity-type").and_then(Value::as_str) {
        Some("item") => Some(format!("Q{numeric_id}")),
        Some("property") => Some(format!("P{numeric_id}")),
        Some("lexeme") => Some(format!("L{numeric_id}")),
        _ => None,
    }
}

/// Extracts the unit entity ID from a Wikidata quantity value.
fn wikidata_quantity_unit_id(value: &Value) -> Option<String> {
    value
        .get("unit")
        .and_then(Value::as_str)
        .and_then(wikidata_entity_id_from_url)
}

/// Extracts an entity ID from a Wikidata entity URL.
fn wikidata_entity_id_from_url(url: &str) -> Option<String> {
    url.rsplit('/')
        .next()
        .and_then(normalize_wikidata_entity_id)
}

/// Chooses a localized Wikidata label and optional description for display.
fn wikidata_localized_entity_value(
    values: Option<&HashMap<String, WikidataLocalizedValue>>,
    language: &str,
) -> Option<String> {
    let values = values?;
    values
        .get(language)
        .or_else(|| values.get("en"))
        .or_else(|| values.values().next())
        .map(|value| value.value.clone())
}

/// Normalizes Wikidata item/property IDs.
fn normalize_wikidata_entity_id(value: &str) -> Option<String> {
    let value = value.trim();
    let mut chars = value.chars();
    let prefix = chars.next()?.to_ascii_uppercase();
    if !matches!(prefix, 'Q' | 'P' | 'L') {
        return None;
    }
    let rest = chars.collect::<String>();
    (!rest.is_empty() && rest.chars().all(|character| character.is_ascii_digit()))
        .then(|| format!("{prefix}{rest}"))
}

/// Builds a Wikidata entity URL.
fn wikidata_entity_url(entity_id: &str) -> String {
    if entity_id.starts_with('P') {
        format!("https://www.wikidata.org/wiki/Property:{entity_id}")
    } else {
        format!("https://www.wikidata.org/wiki/{entity_id}")
    }
}

/// Builds the Wikidata action=info URL for an entity page.
fn wikidata_action_info_url(item: &str) -> String {
    format!("https://www.wikidata.org/w/index.php?title={item}&action=info")
}

/// Builds a Wikidata old revision URL.
fn wikidata_revision_link(revision_id: u64) -> String {
    html_link(
        &format!("https://www.wikidata.org/w/index.php?oldid={revision_id}"),
        &format!("rev {revision_id}"),
    )
}

/// Builds a Wikidata user page URL.
fn wikidata_user_link(user: Option<&str>) -> String {
    let Some(user) = user.map(str::trim).filter(|user| {
        !user.is_empty() && !user.starts_with("http://") && !user.starts_with("https://")
    }) else {
        return html_escape_text("unknown editor");
    };

    html_link(
        &format!(
            "https://www.wikidata.org/wiki/User:{}",
            urlencoding::encode(&user.replace(' ', "_"))
        ),
        user,
    )
}

/// Builds a Wikimedia Commons file URL.
fn commons_file_url(file: &str) -> String {
    let file = file.trim();
    let title = if file.to_ascii_lowercase().starts_with("file:") {
        file.to_string()
    } else {
        format!("File:{file}")
    };
    commons_title_url(&title)
}

/// Checks whether a value is already an absolute HTTP URL.
fn is_absolute_url(value: &str) -> bool {
    value.starts_with("https://") || value.starts_with("http://")
}

/// Builds a Wikipedia old revision URL.
fn revision_link(language: &str, revision_id: u64) -> String {
    html_link(
        &format!("https://{language}.wikipedia.org/w/index.php?oldid={revision_id}"),
        &format!("rev {revision_id}"),
    )
}

/// Builds a Wikimedia Pageviews UI URL for the last 30 days.
fn pageviews_url(language: &str, title: &str) -> String {
    format!(
        "https://pageviews.wmcloud.org/pageviews/?project={language}.wikipedia.org&platform=all-access&agent=user&redirects=0&range=latest-30&pages={}",
        urlencoding::encode(&title.replace(' ', "_"))
    )
}

/// Summarizes daily pageviews returned by MediaWiki.
fn pageviews_from_map(views_by_date: Option<HashMap<String, Option<u64>>>) -> PageViews {
    let views_by_date = views_by_date.unwrap_or_default();
    PageViews {
        days: views_by_date.len(),
        total: views_by_date.values().filter_map(|views| *views).sum(),
        latest: views_by_date
            .iter()
            .filter_map(|(date, views)| Some((date.clone(), (*views)?)))
            .max_by(|(left_date, _), (right_date, _)| left_date.cmp(right_date)),
    }
}

/// Finds a Commons page URL from article pageprops.
fn commons_page_url(page_props: &HashMap<String, String>) -> Option<String> {
    page_props
        .get("commonscat")
        .map(|category| commons_category_url(category))
        .or_else(|| {
            page_props
                .get("commonswiki")
                .map(|title| commons_title_url(title))
        })
}

/// Finds a Commons URL from Wikidata sitelinks or Commons category claims.
fn wikidata_commons_url_from_entity(entity: &WikidataEntity) -> Option<String> {
    entity
        .sitelinks
        .as_ref()
        .and_then(|sitelinks| sitelinks.get("commonswiki"))
        .map(|sitelink| commons_title_url(&sitelink.title))
        .or_else(|| {
            entity
                .claims
                .as_ref()
                .and_then(|claims| claims.get("P373"))
                .and_then(|claims| {
                    claims.iter().find_map(|claim| {
                        claim
                            .mainsnak
                            .datavalue
                            .as_ref()
                            .and_then(|datavalue| datavalue.value.as_str())
                            .map(commons_category_url)
                    })
                })
        })
}

/// Extracts a formatter URL template from a Wikidata property entity.
fn wikidata_formatter_url_from_entity(entity: &WikidataEntity) -> Option<String> {
    entity
        .claims
        .as_ref()?
        .get("P1630")?
        .iter()
        .find_map(|claim| {
            claim
                .mainsnak
                .datavalue
                .as_ref()?
                .value
                .as_str()
                .map(str::trim)
                .filter(|formatter| is_absolute_url(formatter) && formatter.contains("$1"))
                .map(ToOwned::to_owned)
        })
}

/// Builds a Wikimedia Commons category URL.
fn commons_category_url(category: &str) -> String {
    let category = category.trim();
    if category.to_ascii_lowercase().starts_with("category:") {
        commons_title_url(category)
    } else {
        format!(
            "https://commons.wikimedia.org/wiki/Category:{}",
            urlencoding::encode(&category.replace(' ', "_"))
        )
    }
}

/// Builds a Wikimedia Commons title URL.
fn commons_title_url(title: &str) -> String {
    format!(
        "https://commons.wikimedia.org/wiki/{}",
        urlencoding::encode(&title.trim().replace(' ', "_"))
    )
}

/// Builds a Wikipedia user page URL for revision metadata.
fn revision_user_link(language: &str, user: Option<&str>) -> String {
    let Some(user) = user.map(str::trim).filter(|user| {
        !user.is_empty() && !user.starts_with("http://") && !user.starts_with("https://")
    }) else {
        return html_escape_text("unknown editor");
    };

    html_link(
        &format!(
            "https://{language}.wikipedia.org/wiki/User:{}",
            urlencoding::encode(&user.replace(' ', "_"))
        ),
        user,
    )
}

/// Builds a GeoHack map URL for coordinates.
fn coordinate_url(lat: f64, lon: f64) -> String {
    let lat_dir = if lat >= 0.0 { "N" } else { "S" };
    let lon_dir = if lon >= 0.0 { "E" } else { "W" };
    format!(
        "https://geohack.toolforge.org/geohack.php?params={}_{}_{}_{}",
        lat.abs(),
        lat_dir,
        lon.abs(),
        lon_dir
    )
}

/// Parses coordinate text commonly found in infoboxes.
fn parse_coordinate_pair(line: &str) -> Option<(f64, f64)> {
    let re = COORDINATE_DMS_RE.get_or_init(|| {
        Regex::new(
            r#"(?ix)
            (?P<lat_deg>\d{1,3})\s*°\s*
            (?:(?P<lat_min>\d{1,2})\s*[′']\s*)?
            (?:(?P<lat_sec>\d{1,2})\s*[″"]\s*)?
            (?P<lat_dir>N|S|С\.?\s*Ш\.?|Ю\.?\s*Ш\.?)\s+
            (?P<lon_deg>\d{1,3})\s*°\s*
            (?:(?P<lon_min>\d{1,2})\s*[′']\s*)?
            (?:(?P<lon_sec>\d{1,2})\s*[″"]\s*)?
            (?P<lon_dir>E|W|В\.?\s*Д\.?|З\.?\s*Д\.?)
            "#,
        )
        .expect("coordinate regex is valid")
    });
    let captures = re.captures(line)?;
    let lat = coordinate_from_captures(&captures, "lat", 90.0)?;
    let lon = coordinate_from_captures(&captures, "lon", 180.0)?;
    Some((lat, lon))
}

/// Converts DMS regex captures into decimal coordinates.
fn coordinate_from_captures(
    captures: &regex::Captures<'_>,
    prefix: &str,
    max_degrees: f64,
) -> Option<f64> {
    let degrees = capture_number(captures, &format!("{prefix}_deg"))?;
    let minutes = capture_number(captures, &format!("{prefix}_min")).unwrap_or(0.0);
    let seconds = capture_number(captures, &format!("{prefix}_sec")).unwrap_or(0.0);
    if degrees > max_degrees || minutes >= 60.0 || seconds >= 60.0 {
        return None;
    }

    let direction = captures.name(&format!("{prefix}_dir"))?.as_str();
    let sign = if coordinate_direction_is_negative(direction) {
        -1.0
    } else {
        1.0
    };
    Some(sign * (degrees + minutes / 60.0 + seconds / 3600.0))
}

/// Reads a numeric regex capture.
fn capture_number(captures: &regex::Captures<'_>, name: &str) -> Option<f64> {
    captures.name(name)?.as_str().parse::<f64>().ok()
}

/// Checks whether a coordinate direction is south or west.
fn coordinate_direction_is_negative(direction: &str) -> bool {
    let direction = direction.trim().to_lowercase();
    direction.starts_with('s')
        || direction.starts_with('w')
        || direction.starts_with('ю')
        || direction.starts_with('з')
}

/// Splits plain text into Telegram-sized chunks.
#[cfg(test)]
fn split_telegram_text(text: &str, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    if text.chars().count() <= limit {
        return vec![text.to_string()];
    }

    let mut parts = Vec::new();
    let mut current = String::new();

    for paragraph in text.split("\n\n") {
        let paragraph = paragraph.trim();
        if paragraph.is_empty() {
            continue;
        }

        let separator = if current.is_empty() { "" } else { "\n\n" };
        if current.chars().count() + separator.chars().count() + paragraph.chars().count() <= limit
        {
            current.push_str(separator);
            current.push_str(paragraph);
            continue;
        }

        if !current.is_empty() {
            parts.push(current);
            current = String::new();
        }

        let mut paragraph_chunks = split_long_paragraph(paragraph, limit)
            .into_iter()
            .peekable();
        while let Some(chunk) = paragraph_chunks.next() {
            if paragraph_chunks.peek().is_some() || chunk.chars().count() == limit {
                parts.push(chunk);
            } else {
                current = chunk;
            }
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

/// Splits Telegram HTML by lines while keeping links balanced.
fn split_html_lines_for_telegram(html: &str, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    let mut parts = Vec::new();
    let mut current = String::new();

    for block in split_html_blocks_for_telegram(html, limit) {
        if current.is_empty() {
            current = block;
            continue;
        }

        if current.chars().count() + 1 + block.chars().count() <= limit {
            current.push('\n');
            current.push_str(&block);
        } else {
            parts.push(current);
            current = block;
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    if parts.is_empty() {
        parts.push(String::new());
    }

    parts
}

/// Splits Telegram HTML blocks, preserving `<pre>` blocks when possible.
fn split_html_blocks_for_telegram(html: &str, limit: usize) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut pre_lines: Option<Vec<String>> = None;

    for line in html.lines() {
        if let Some(lines) = pre_lines.as_mut() {
            lines.push(line.to_string());
            if line.contains(TELEGRAM_PRE_CLOSE)
                && let Some(lines) = pre_lines.take()
            {
                blocks.extend(split_pre_block_for_telegram(&lines.join("\n"), limit));
            }
            continue;
        }

        if line.starts_with(TELEGRAM_PRE_OPEN) {
            if line.contains(TELEGRAM_PRE_CLOSE) {
                blocks.extend(split_pre_block_for_telegram(line, limit));
            } else {
                pre_lines = Some(vec![line.to_string()]);
            }
            continue;
        }

        blocks.push(split_plain_html_line_for_telegram(line, limit));
    }

    if let Some(lines) = pre_lines {
        blocks.extend(
            lines
                .into_iter()
                .map(|line| split_plain_html_line_for_telegram(&line, limit)),
        );
    }

    blocks
}

/// Splits or escapes one long plain HTML line.
fn split_plain_html_line_for_telegram(line: &str, limit: usize) -> String {
    if line.chars().count() > limit {
        html_escape_text(&truncate_for_telegram(&html_to_plain_text(line), limit))
    } else {
        line.to_string()
    }
}

/// Splits an oversized Telegram `<pre>` block into valid `<pre>` chunks.
fn split_pre_block_for_telegram(block: &str, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    if block.chars().count() <= limit {
        return vec![block.to_string()];
    }

    let Some(inner) = block
        .strip_prefix(TELEGRAM_PRE_OPEN)
        .and_then(|value| value.strip_suffix(TELEGRAM_PRE_CLOSE))
    else {
        return block
            .lines()
            .map(|line| split_plain_html_line_for_telegram(line, limit))
            .collect();
    };

    let overhead = TELEGRAM_PRE_OPEN.chars().count() + TELEGRAM_PRE_CLOSE.chars().count();
    let inner_limit = limit.saturating_sub(overhead).max(1);
    let mut parts = Vec::new();
    let mut current = String::new();

    for line in inner.lines() {
        let line = if line.chars().count() > inner_limit {
            html_escape_text(&truncate_for_telegram(
                &html_to_plain_text(line),
                inner_limit,
            ))
        } else {
            line.to_string()
        };

        if current.is_empty() {
            current = line;
            continue;
        }

        if overhead + current.chars().count() + 1 + line.chars().count() <= limit {
            current.push('\n');
            current.push_str(&line);
        } else {
            parts.push(wrap_pre_block(&current));
            current = line;
        }
    }

    if !current.is_empty() {
        parts.push(wrap_pre_block(&current));
    }

    if parts.is_empty() {
        parts.push(wrap_pre_block(""));
    }

    parts
}

/// Wraps text in a Telegram `<pre>` block.
fn wrap_pre_block(inner: &str) -> String {
    format!("{TELEGRAM_PRE_OPEN}{inner}{TELEGRAM_PRE_CLOSE}")
}

/// Splits a long paragraph by words for tests and splitter behavior.
#[cfg(test)]
fn split_long_paragraph(paragraph: &str, limit: usize) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    for word in paragraph.split_whitespace() {
        let separator = if current.is_empty() { "" } else { " " };
        if current.chars().count() + separator.chars().count() + word.chars().count() <= limit {
            current.push_str(separator);
            current.push_str(word);
            continue;
        }

        if !current.is_empty() {
            parts.push(current);
            current = String::new();
        }

        if word.chars().count() > limit {
            parts.extend(split_long_word(word, limit));
        } else {
            current.push_str(word);
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

/// Splits an oversized word by character count.
#[cfg(test)]
fn split_long_word(word: &str, limit: usize) -> Vec<String> {
    let chars = word.chars().collect::<Vec<_>>();
    chars
        .chunks(limit)
        .map(|chunk| chunk.iter().collect::<String>())
        .collect()
}

/// Truncates text to a Telegram-safe character count.
fn truncate_for_telegram(value: &str, max_chars: usize) -> String {
    let count = value.chars().count();
    if count <= max_chars {
        return value.to_string();
    }

    let ellipsis = "...";
    let keep = max_chars.saturating_sub(ellipsis.chars().count());
    let mut truncated = value.chars().take(keep).collect::<String>();
    truncated.push_str(ellipsis);
    truncated
}

/// Extracts a cleaned infobox text block from parsed article HTML.
fn extract_infobox_text(language: &str, html: &str) -> Option<String> {
    let text = extract_infobox_text_from_dom(language, html).or_else(|| {
        let re = INFOBOX_TABLE_RE.get_or_init(|| {
            Regex::new(r#"(?is)<table\b[^>]*class\s*=\s*["'][^"']*\binfobox\b[^"']*["'][^>]*>"#)
                .expect("infobox regex is valid")
        });
        let table_match = re.find(html)?;
        let table_html =
            &html[table_match.start()..find_matching_table_end(html, table_match.start())?];
        let text = clean_infobox_text(&html_to_text(table_html));
        Some(
            text.lines()
                .map(html_escape_text)
                .collect::<Vec<_>>()
                .join("\n"),
        )
    })?;
    (!text.is_empty()).then_some(text)
}

/// Extracts infobox text using the DOM parser before falling back to regex.
fn extract_infobox_text_from_dom(language: &str, html: &str) -> Option<String> {
    let document = Html::parse_fragment(html);
    let infobox_selector = Selector::parse("table.infobox").expect("infobox selector is valid");
    let infobox = document.select(&infobox_selector).next()?;
    let mut rows = Vec::new();
    collect_table_rows(infobox, &mut rows);

    let lines = rows
        .into_iter()
        .filter_map(|row| render_infobox_row(language, row))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    (!lines.is_empty()).then(|| lines.join("\n"))
}

/// Renders one infobox table row, dropping empty labels and noise.
fn render_infobox_row(language: &str, row: ElementRef<'_>) -> Option<String> {
    if element_has_class_part(row, &["metadata", "navbox", "noprint"]) {
        return None;
    }

    let cells = row
        .children()
        .filter_map(ElementRef::wrap)
        .filter(|cell| matches!(cell.value().name(), "th" | "td"))
        .filter(|cell| {
            !element_has_class_part(
                *cell,
                &[
                    "infobox-below",
                    "metadata",
                    "navbox",
                    "noprint",
                    "mw-empty-elt",
                ],
            )
        })
        .map(|cell| {
            (
                cell.value().name() == "th",
                infobox_cell_html(language, cell),
            )
        })
        .filter(|(_, text)| !text.is_empty())
        .collect::<Vec<_>>();

    match cells.as_slice() {
        [] => None,
        [(is_header, text)] => {
            let plain = html_to_plain_text(text);
            if *is_header || is_meaningful_unlabeled_infobox_line(&plain) {
                Some(text.clone())
            } else {
                None
            }
        }
        [(true, label), rest @ ..] => {
            let label = html_to_plain_text(label);
            let label = label.trim().trim_end_matches(':').trim();
            let value = rest
                .iter()
                .map(|(_, text)| text.as_str())
                .collect::<Vec<_>>()
                .join(" / ");
            let value_is_empty = html_to_plain_text(&value).trim().is_empty();
            (!label.is_empty() && !value_is_empty)
                .then(|| format!("{}: {value}", html_escape_text(label)))
        }
        cells => {
            let text = cells
                .iter()
                .map(|(_, text)| text.as_str())
                .collect::<Vec<_>>()
                .join(" / ");
            is_meaningful_unlabeled_infobox_line(&html_to_plain_text(&text)).then_some(text)
        }
    }
}

/// Renders one infobox cell as compact Telegram HTML.
fn infobox_cell_html(language: &str, cell: ElementRef<'_>) -> String {
    clean_infobox_text(&trim_html_spacing(&render_inline_element(language, cell)))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" / ")
}

/// Keeps unlabeled infobox rows only when they look meaningful.
fn is_meaningful_unlabeled_infobox_line(line: &str) -> bool {
    parse_coordinate_pair(line).is_some() || line.chars().count() >= 24
}

/// Extracts the first useful infobox image.
fn extract_infobox_image(html: &str) -> Option<MediaItem> {
    let document = Html::parse_fragment(html);
    let infobox_selector = Selector::parse("table.infobox").expect("infobox selector is valid");
    let image_selector = Selector::parse("img[src]").expect("image selector is valid");

    for infobox in document.select(&infobox_selector) {
        for image in infobox.select(&image_selector) {
            if element_has_class_part(image, &["mw-kartographer", "locmap"]) {
                continue;
            }
            let alt = image.value().attr("alt").unwrap_or_default();
            if alt.eq_ignore_ascii_case("map") {
                continue;
            }

            let Some(src) = image.value().attr("src").and_then(normalize_image_src) else {
                continue;
            };
            if !looks_like_image_url(&src) {
                continue;
            }
            let original =
                wikimedia_original_image_url_from_thumbnail(&src).unwrap_or_else(|| src.clone());
            let fallback_url = (original != src)
                .then(|| {
                    wikimedia_thumbnail_url_with_width(&src, ARTICLE_IMAGE_FALLBACK_THUMB_WIDTH)
                })
                .flatten()
                .or_else(|| (original != src).then_some(src));

            let title = image
                .ancestors()
                .filter_map(ElementRef::wrap)
                .find(|element| element.value().name() == "a")
                .and_then(|link| link.value().attr("title"))
                .unwrap_or("Infobox image")
                .to_string();

            return Some(MediaItem {
                title,
                url: original,
                fallback_url,
                caption: None,
                mime: None,
                width: None,
                height: None,
            });
        }
    }

    None
}

/// Converts MediaWiki imageinfo into a media item.
fn media_item_from_image_info_page(page: ImageInfoPage) -> Option<MediaItem> {
    let info = page.imageinfo?.into_iter().next()?;
    let fallback_url = info
        .mime
        .as_deref()
        .is_some_and(|mime| mime.starts_with("image/"))
        .then(|| info.thumburl.clone())
        .flatten()
        .filter(|thumburl| thumburl != &info.url);

    Some(MediaItem {
        title: page.title,
        url: info.url,
        fallback_url,
        caption: None,
        mime: info.mime,
        width: info.width,
        height: info.height,
    })
}

/// Normalizes protocol-relative and absolute image sources.
fn normalize_image_src(src: &str) -> Option<String> {
    let src = html_escape::decode_html_entities(src.trim()).to_string();
    if src.is_empty() || src.starts_with("data:") {
        return None;
    }
    if src.starts_with("https://") || src.starts_with("http://") {
        Some(src)
    } else {
        src.strip_prefix("//").map(|rest| format!("https://{rest}"))
    }
}

/// Reconstructs an original Commons URL from a thumbnail URL.
fn wikimedia_original_image_url_from_thumbnail(url: &str) -> Option<String> {
    let (prefix, rest) = url.split_once("/thumb/")?;
    if !prefix.ends_with("upload.wikimedia.org/wikipedia/commons") {
        return None;
    }

    let original_path = rest.rsplit_once('/')?.0;
    Some(format!("{prefix}/{original_path}"))
}

/// Rewrites a Commons thumbnail URL to request a larger width.
fn wikimedia_thumbnail_url_with_width(url: &str, width: usize) -> Option<String> {
    let (prefix, rest) = url.split_once("/thumb/")?;
    if !prefix.ends_with("upload.wikimedia.org/wikipedia/commons") {
        return None;
    }

    let (original_path, thumbnail_file) = rest.rsplit_once('/')?;
    let suffix_start = thumbnail_file.find("px-")? + "px-".len();
    let suffix = thumbnail_file.get(suffix_start..)?;
    Some(format!("{prefix}/thumb/{original_path}/{width}px-{suffix}"))
}

/// Checks whether a URL looks like an image file Telegram can send.
fn looks_like_image_url(url: &str) -> bool {
    let without_query = url.split('?').next().unwrap_or(url).to_ascii_lowercase();
    without_query.ends_with(".jpg")
        || without_query.ends_with(".jpeg")
        || without_query.ends_with(".png")
        || without_query.ends_with(".webp")
        || without_query.ends_with(".gif")
        || without_query.ends_with(".tif")
        || without_query.ends_with(".tiff")
        || without_query.ends_with(".svg")
}

/// Cleans CSS, map captions, sister-project boilerplate, and empty rows from an
/// infobox text block.
fn clean_infobox_text(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .filter(|line| {
            let plain = html_to_plain_text(line);
            let plain = plain.trim();
            !plain.is_empty()
                && !is_empty_infobox_label(plain)
                && !is_infobox_map_caption(plain)
                && !is_infobox_sister_project_line(plain)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Detects empty infobox labels that MediaWiki exposes as text.
fn is_empty_infobox_label(line: &str) -> bool {
    line.trim()
        .strip_suffix(':')
        .is_some_and(|label| !label.trim().is_empty())
}

/// Detects map placeholder captions that do not make sense without the map.
fn is_infobox_map_caption(line: &str) -> bool {
    let line = line.trim().to_ascii_lowercase();
    line.starts_with("interactive map")
        || line.starts_with("show map")
        || line.starts_with("show all")
}

/// Detects sister-project infobox lines such as Commons links.
fn is_infobox_sister_project_line(line: &str) -> bool {
    let line = line.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = line.to_lowercase();
    (lower.contains("викисклад") && lower.contains("медиафайл"))
        || lower == "wikimedia commons"
        || lower == "media files at wikimedia commons"
        || lower == "media related to wikimedia commons"
}

/// Finds the closing table tag matching a table start in raw HTML.
fn find_matching_table_end(html: &str, start: usize) -> Option<usize> {
    let lower = html.to_ascii_lowercase();
    let mut cursor = start;
    let mut depth = 0usize;

    loop {
        let next_open = lower[cursor..].find("<table").map(|index| cursor + index);
        let next_close = lower[cursor..].find("</table>").map(|index| cursor + index);

        match (next_open, next_close) {
            (Some(open), Some(close)) if open < close => {
                depth += 1;
                cursor = open + "<table".len();
            }
            (_, Some(close)) => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                cursor = close + "</table>".len();
                if depth == 0 {
                    return Some(cursor);
                }
            }
            _ => return None,
        }
    }
}

/// Converts HTML to plain text for fallback parsing.
fn html_to_text(html: &str) -> String {
    let without_style_script = STYLE_SCRIPT_RE
        .get_or_init(|| {
            Regex::new(
                r"(?is)<style\b[^>]*>.*?</style>|<script\b[^>]*>.*?</script>|<noscript\b[^>]*>.*?</noscript>",
            )
            .expect("style/script regex is valid")
        })
        .replace_all(html, "");
    let without_sup = SUP_RE
        .get_or_init(|| Regex::new(r"(?is)<sup\b[^>]*>.*?</sup>").expect("sup regex is valid"))
        .replace_all(&without_style_script, "");
    let with_breaks = without_sup
        .replace("<br />", "\n")
        .replace("<br/>", "\n")
        .replace("<br>", "\n")
        .replace("</tr>", "\n")
        .replace("</li>", "\n")
        .replace("</th>", ": ")
        .replace("</td>", "; ");
    let without_tags = TAG_RE
        .get_or_init(|| Regex::new(r"(?is)<[^>]+>").expect("tag regex is valid"))
        .replace_all(&with_breaks, " ");
    let decoded = html_escape::decode_html_entities(&without_tags);
    normalize_text_lines(&decoded)
}

/// Normalizes plain text lines by trimming and dropping empties.
fn normalize_text_lines(value: &str) -> String {
    let mut lines = Vec::new();
    for line in value.lines() {
        let normalized = line
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .trim_matches(';')
            .trim()
            .to_string();
        if !normalized.is_empty() {
            lines.push(normalized);
        }
    }
    lines.join("\n")
}

/// Checks whether a title is likely to represent a media file.
fn looks_like_image_or_audio(title: &str) -> bool {
    let title = title.to_ascii_lowercase();
    title.ends_with(".jpg")
        || title.ends_with(".jpeg")
        || title.ends_with(".png")
        || title.ends_with(".webp")
        || title.ends_with(".gif")
        || title.ends_with(".tif")
        || title.ends_with(".tiff")
        || title.ends_with(".ogg")
        || title.ends_with(".oga")
        || title.ends_with(".opus")
        || title.ends_with(".mp3")
        || title.ends_with(".wav")
        || title.ends_with(".flac")
}

/// Checks whether a media item is suitable for the article image gallery.
fn is_gallery_image(item: &MediaItem) -> bool {
    let is_image = item
        .mime
        .as_deref()
        .is_some_and(|mime| mime.starts_with("image/"))
        || looks_like_image_url(&item.url);
    is_image && !looks_like_icon_media_item(item)
}

/// Selects up to the configured gallery limit after dedupe and icon filtering.
fn article_gallery_images(
    media: &ArticleMedia,
    infobox_image: Option<&MediaItem>,
) -> Vec<MediaItem> {
    let mut images = Vec::new();
    if let Some(image) = infobox_image.filter(|image| is_gallery_image(image)) {
        push_unique_media_item(&mut images, image.clone());
    }
    if let Some(image) = media
        .lead_image
        .as_ref()
        .filter(|image| is_gallery_image(image))
    {
        push_unique_media_item(&mut images, image.clone());
    }
    for image in media.images.iter().filter(|image| is_gallery_image(image)) {
        push_unique_media_item(&mut images, image.clone());
        if images.len() >= ARTICLE_IMAGE_GALLERY_LIMIT {
            break;
        }
    }
    images.truncate(ARTICLE_IMAGE_GALLERY_LIMIT);
    images
}

/// Removes media items that were already sent inline with article sections.
fn filter_excluded_media_items(
    images: Vec<MediaItem>,
    excluded_images: &[MediaItem],
) -> Vec<MediaItem> {
    if excluded_images.is_empty() {
        return images;
    }

    let excluded_urls = excluded_images
        .iter()
        .map(|image| image.url.as_str())
        .collect::<HashSet<_>>();
    let excluded_titles = excluded_images
        .iter()
        .map(|image| normalized_media_title(&image.title))
        .filter(|title| !title.is_empty() && title != "article image" && title != "infobox image")
        .collect::<HashSet<_>>();

    images
        .into_iter()
        .filter(|image| {
            !excluded_urls.contains(image.url.as_str())
                && !excluded_titles.contains(&normalized_media_title(&image.title))
        })
        .collect()
}

/// Detects icon-like images by dimensions or title.
fn looks_like_icon_media_item(item: &MediaItem) -> bool {
    looks_like_icon_title(&item.title)
        || looks_like_icon_title(&item.url)
        || item.width.zip(item.height).is_some_and(|(width, height)| {
            width < MIN_ICON_FILTER_SIDE_PX && height < MIN_ICON_FILTER_SIDE_PX
        })
}

/// Detects common icon/logo placeholder filenames.
fn looks_like_icon_title(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase().replace([' ', '-'], "_");
    [
        "ambox",
        "commons_logo",
        "crystal_clear",
        "edit_icon",
        "gnome_",
        "icon",
        "map_marker",
        "nuvola",
        "oojs_ui",
        "question_book",
        "sound_icon",
        "speaker_icon",
        "stub",
        "symbol",
        "wikidata",
        "wikimedia_logo",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

/// Appends a media item unless an equivalent URL/title is already present.
fn push_unique_media_item(items: &mut Vec<MediaItem>, item: MediaItem) {
    if !items
        .iter()
        .any(|existing| media_items_match(existing, &item))
    {
        items.push(item);
    }
}

/// Compares media items for deduplication.
fn media_items_match(left: &MediaItem, right: &MediaItem) -> bool {
    left.url == right.url
        || normalized_media_title(&left.title) == normalized_media_title(&right.title)
}

/// Normalizes file titles for media deduplication.
fn normalized_media_title(title: &str) -> String {
    let title = title.trim();
    let title = title
        .strip_prefix("File:")
        .or_else(|| title.strip_prefix("file:"))
        .or_else(|| title.strip_prefix("Файл:"))
        .or_else(|| title.strip_prefix("файл:"))
        .unwrap_or(title);
    title
        .trim()
        .replace('_', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

#[derive(Debug, Deserialize)]
struct Update {
    update_id: i64,
    message: Option<Message>,
    callback_query: Option<CallbackQuery>,
    inline_query: Option<InlineQuery>,
}

#[derive(Debug, Deserialize)]
struct Message {
    chat: Chat,
    from: Option<User>,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Chat {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct User {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
    id: String,
    from: User,
    message: Option<Message>,
    data: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InlineQuery {
    id: String,
    from: User,
    query: String,
}

#[derive(Debug, Clone, PartialEq)]
struct ArticleButton {
    page_id: u64,
    title: String,
    size: Option<usize>,
    wordcount: Option<usize>,
    snippet: Option<String>,
    is_disambiguation: bool,
}

#[derive(Debug, Clone)]
struct ArticleContent {
    title: String,
    extract: String,
    length: Option<usize>,
    infobox: Option<String>,
    infobox_image: Option<MediaItem>,
    body_html: Option<String>,
    body_sections: Vec<ArticleBodySection>,
    references_html: Option<String>,
    body_links: Vec<NavLink>,
    nav_templates: Vec<NavTemplate>,
    is_disambiguation: bool,
    disambiguation_groups: Vec<DisambiguationLinkGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArticleBodySection {
    heading: Option<ArticleBodyHeading>,
    html: String,
    images: Vec<MediaItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArticleBodyHeading {
    title: String,
    anchor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NavTemplate {
    title: String,
    title_url: Option<String>,
    links: Vec<NavLink>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NavLink {
    label: String,
    page_title: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DisambiguationLinkGroup {
    title: String,
    links: Vec<NavLink>,
}

#[derive(Debug, Clone)]
struct ArticleButtonGroup {
    title: String,
    buttons: Vec<ArticleButton>,
}

#[derive(Debug, Clone)]
struct NavTemplateButtons {
    title: String,
    title_url: Option<String>,
    buttons: Vec<ArticleButton>,
}

#[derive(Debug, Clone)]
struct Category {
    title: String,
    article_count: Option<usize>,
}

#[derive(Debug, Clone)]
struct CategoryArticlePage {
    articles: Vec<ArticleButton>,
    page_index: usize,
    has_next: bool,
}

#[derive(Debug, Clone)]
struct CategorySubcategoryPage {
    categories: Vec<Category>,
    page_index: usize,
    has_next: bool,
}

#[derive(Debug, Clone)]
struct CategoryMemberTitlePage {
    members: Vec<CategoryMemberDto>,
    has_next: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum CategoryPageKind {
    Articles,
    Subcategories,
}

impl CategoryPageKind {
    fn callback_code(self) -> &'static str {
        match self {
            Self::Articles => "a",
            Self::Subcategories => "s",
        }
    }

    fn from_callback_code(value: &str) -> Option<Self> {
        match value {
            "a" => Some(Self::Articles),
            "s" => Some(Self::Subcategories),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct CategoryOverview {
    page_id: Option<u64>,
    article_count: Option<usize>,
    subcategory_count: Option<usize>,
    description: Option<String>,
    parent_categories: Vec<Category>,
}

#[derive(Debug, Clone)]
struct ArticleMetadata {
    language: String,
    info: ArticleInfo,
    commons_url: Option<String>,
    wikidata_property_count: Option<usize>,
    revisions: Vec<Revision>,
    external_links: Vec<String>,
    language_links: Vec<LanguageLink>,
    pageviews: PageViews,
}

#[derive(Debug, Clone)]
struct WikidataClaims {
    item: String,
    label: Option<String>,
    description: Option<String>,
    property_count: usize,
    properties: Vec<WikidataPropertyClaims>,
}

#[derive(Debug, Clone)]
struct WikidataPropertyClaims {
    property_id: String,
    property_label: String,
    total_values: usize,
    values: Vec<String>,
}

#[derive(Debug, Clone)]
struct WikidataLabelInfo {
    label: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Clone)]
struct WikidataPageMetadata {
    item: String,
    page_id: Option<u64>,
    title: String,
    full_url: Option<String>,
    length: Option<usize>,
    touched: Option<String>,
    last_revision_id: Option<u64>,
    revisions: Vec<Revision>,
}

struct ArticleMetadataParts {
    info: ArticleInfo,
    revisions: Vec<Revision>,
    external_links: Vec<String>,
    language_links: Vec<LanguageLink>,
    pageviews: PageViews,
}

#[derive(Debug, Clone, Default)]
struct PageViews {
    days: usize,
    total: u64,
    latest: Option<(String, u64)>,
}

#[derive(Debug, Clone)]
struct ArticleInfo {
    page_id: Option<u64>,
    title: String,
    full_url: Option<String>,
    length: Option<usize>,
    touched: Option<String>,
    last_revision_id: Option<u64>,
    page_props: HashMap<String, String>,
    coordinates: Vec<Coordinate>,
}

#[derive(Debug, Clone)]
struct ArticleMedia {
    lead_image: Option<MediaItem>,
    images: Vec<MediaItem>,
    audio: Option<MediaItem>,
}

struct ArticleMediaSend<'a> {
    language: &'a str,
    page_id: u64,
    title: &'a str,
    media: &'a ArticleMedia,
    infobox_image: Option<&'a MediaItem>,
    excluded_images: &'a [MediaItem],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MediaItem {
    title: String,
    url: String,
    fallback_url: Option<String>,
    caption: Option<String>,
    mime: Option<String>,
    width: Option<u64>,
    height: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    query: SearchQuery,
}

#[derive(Debug, Deserialize)]
struct SearchQuery {
    search: Vec<SearchHit>,
}

#[derive(Debug, Deserialize)]
struct SearchHit {
    title: String,
}

#[derive(Debug, Deserialize)]
struct CategoryInfoResponse {
    query: Option<InfoQuery>,
}

#[derive(Debug, Deserialize)]
struct CategoryMembersResponse {
    query: Option<CategoryMembersQuery>,
    #[serde(rename = "continue")]
    continuation: Option<CategoryMembersContinue>,
}

#[derive(Debug, Deserialize)]
struct CategoryMembersQuery {
    categorymembers: Vec<CategoryMemberDto>,
}

#[derive(Debug, Clone, Deserialize)]
struct CategoryMemberDto {
    title: String,
}

#[derive(Debug, Deserialize)]
struct CategoryMembersContinue {
    cmcontinue: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ParseResponse {
    parse: Option<ParsedPage>,
}

#[derive(Debug, Deserialize)]
struct ParsedPage {
    title: Option<String>,
    text: Option<String>,
    properties: Option<HashMap<String, Value>>,
}

#[derive(Debug, Deserialize)]
struct InfoResponse {
    query: InfoQuery,
}

#[derive(Debug, Deserialize)]
struct InfoQuery {
    pages: Vec<InfoPage>,
}

#[derive(Debug, Deserialize)]
struct InfoPage {
    pageid: Option<u64>,
    title: String,
    index: Option<usize>,
    extract: Option<String>,
    fullurl: Option<String>,
    length: Option<usize>,
    touched: Option<String>,
    lastrevid: Option<u64>,
    pageprops: Option<HashMap<String, String>>,
    coordinates: Option<Vec<Coordinate>>,
    categoryinfo: Option<CategoryInfoDto>,
    categories: Option<Vec<CategoryDto>>,
    revisions: Option<Vec<Revision>>,
    extlinks: Option<Vec<ExternalLink>>,
    langlinks: Option<Vec<LanguageLink>>,
    pageviews: Option<HashMap<String, Option<u64>>>,
}

#[derive(Debug, Deserialize)]
struct CategoryInfoDto {
    #[serde(default)]
    pages: usize,
    #[serde(default)]
    subcats: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct Coordinate {
    lat: f64,
    lon: f64,
    globe: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct Revision {
    revid: Option<u64>,
    timestamp: Option<String>,
    user: Option<String>,
    comment: Option<String>,
    size: Option<usize>,
    tags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ExternalLink {
    url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct LanguageLink {
    lang: String,
    langname: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CategoryDto {
    title: String,
}

#[derive(Debug, Deserialize)]
struct PageMediaResponse {
    query: PageMediaQuery,
}

#[derive(Debug, Deserialize)]
struct PageMediaQuery {
    pages: Vec<PageMediaPage>,
}

#[derive(Debug, Deserialize)]
struct PageMediaPage {
    pageimage: Option<String>,
    thumbnail: Option<Thumbnail>,
    images: Option<Vec<FileTitle>>,
}

#[derive(Debug, Deserialize)]
struct Thumbnail {
    source: String,
    width: Option<u64>,
    height: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct FileTitle {
    title: String,
}

#[derive(Debug, Deserialize)]
struct ImageInfoResponse {
    query: ImageInfoQuery,
}

#[derive(Debug, Deserialize)]
struct ImageInfoQuery {
    pages: Vec<ImageInfoPage>,
}

#[derive(Debug, Deserialize)]
struct ImageInfoPage {
    title: String,
    imageinfo: Option<Vec<ImageInfo>>,
}

#[derive(Debug, Deserialize)]
struct ImageInfo {
    url: String,
    mime: Option<String>,
    width: Option<u64>,
    height: Option<u64>,
    thumburl: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WikidataEntitiesResponse {
    entities: HashMap<String, WikidataEntity>,
}

#[derive(Debug, Clone, Deserialize)]
struct WikidataEntity {
    sitelinks: Option<HashMap<String, WikidataSitelink>>,
    claims: Option<HashMap<String, Vec<WikidataClaim>>>,
    labels: Option<HashMap<String, WikidataLocalizedValue>>,
    descriptions: Option<HashMap<String, WikidataLocalizedValue>>,
}

#[derive(Debug, Clone, Deserialize)]
struct WikidataSitelink {
    title: String,
}

#[derive(Debug, Clone, Deserialize)]
struct WikidataClaim {
    mainsnak: WikidataSnak,
}

#[derive(Debug, Clone, Deserialize)]
struct WikidataSnak {
    datatype: Option<String>,
    snaktype: Option<String>,
    datavalue: Option<WikidataDataValue>,
}

#[derive(Debug, Clone, Deserialize)]
struct WikidataDataValue {
    value: Value,
}

#[derive(Debug, Clone, Deserialize)]
struct WikidataLocalizedValue {
    value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARMIES_OF_EXIGO_HTML: &str =
        include_str!("../tests/fixtures/wikipedia/armies_of_exigo.html");
    const FLAG_OF_ADJARA_HTML: &str =
        include_str!("../tests/fixtures/wikipedia/flag_of_adjara.html");
    const MILL_FARM_INN_HTML: &str = include_str!("../tests/fixtures/wikipedia/mill_farm_inn.html");
    const PORTA_BATUMI_TOWER_HTML: &str =
        include_str!("../tests/fixtures/wikipedia/porta_batumi_tower.html");
    const VOSTOK_1_RU_HTML: &str = include_str!("../tests/fixtures/wikipedia/vostok_1_ru.html");

    #[test]
    fn parses_language_prefixed_search() {
        let request = parse_search_request("lang:fr Albert Camus", "en").unwrap();
        assert_eq!(
            request,
            SearchRequest {
                language: "fr".to_string(),
                query: "Albert Camus".to_string()
            }
        );
    }

    #[test]
    fn detects_common_scripts() {
        assert_eq!(detect_language("თბილისი", "en"), "ka");
        assert_eq!(detect_language("Москва", "en"), "ru");
        assert_eq!(detect_language("Беларусь і Мінск", "en"), "be");
        assert_eq!(detect_language("東京", "en"), "zh");
        assert_eq!(detect_language("Albert Camus", "en"), "en");
    }

    #[test]
    fn parses_favorite_categories() {
        let categories =
            parse_favorite_categories(Some("en:Physics, ru:Категория:Наука | Mathematics"), "en")
                .unwrap();

        assert_eq!(
            categories,
            vec![
                FavoriteCategory {
                    language: "en".to_string(),
                    title: "Category:Physics".to_string()
                },
                FavoriteCategory {
                    language: "ru".to_string(),
                    title: "Категория:Наука".to_string()
                },
                FavoriteCategory {
                    language: "en".to_string(),
                    title: "Category:Mathematics".to_string()
                }
            ]
        );
    }

    #[test]
    fn article_category_callback_round_trips_source_article_and_index() {
        let data = article_category_callback_data("en", 42, 3);
        assert_eq!(data, "article_category:en:42:3");
        let action = parse_callback_data(&data, &[]).unwrap();
        match action {
            CallbackAction::ArticleCategory {
                language,
                page_id,
                index,
            } => {
                assert_eq!(language, "en");
                assert_eq!(page_id, 42);
                assert_eq!(index, 3);
            }
            CallbackAction::Article { .. }
            | CallbackAction::Category { .. }
            | CallbackAction::CategoryPage { .. }
            | CallbackAction::Wikidata { .. }
            | CallbackAction::Images { .. } => {
                panic!("expected article category callback")
            }
        }
    }

    #[test]
    fn wikidata_callback_round_trips_item() {
        let data = wikidata_callback_data("en", "q686963").unwrap();
        assert_eq!(data, "wikidata:en:Q686963");

        let action = parse_callback_data(&data, &[]).unwrap();
        match action {
            CallbackAction::Wikidata { language, item } => {
                assert_eq!(language, "en");
                assert_eq!(item, "Q686963");
            }
            CallbackAction::Article { .. }
            | CallbackAction::Category { .. }
            | CallbackAction::CategoryPage { .. }
            | CallbackAction::ArticleCategory { .. }
            | CallbackAction::Images { .. } => {
                panic!("expected Wikidata callback")
            }
        }
    }

    #[test]
    fn article_images_callback_round_trips_page_id() {
        let data = article_images_callback_data("EN", 12345).unwrap();
        assert_eq!(data, "images:en:12345");

        let action = parse_callback_data(&data, &[]).unwrap();
        match action {
            CallbackAction::Images { language, page_id } => {
                assert_eq!(language, "en");
                assert_eq!(page_id, 12345);
            }
            CallbackAction::Article { .. }
            | CallbackAction::Category { .. }
            | CallbackAction::CategoryPage { .. }
            | CallbackAction::ArticleCategory { .. }
            | CallbackAction::Wikidata { .. } => {
                panic!("expected article images callback")
            }
        }
    }

    #[test]
    fn category_page_callback_round_trips_page_kind_and_index() {
        let data =
            category_page_callback_data("EN", 98765, CategoryPageKind::Articles, 2, Some(10))
                .unwrap();
        assert_eq!(data, "catpage:en:98765:a:2:10");

        let action = parse_callback_data(&data, &[]).unwrap();
        match action {
            CallbackAction::CategoryPage {
                language,
                page_id,
                kind,
                page_index,
                total_pages,
            } => {
                assert_eq!(language, "en");
                assert_eq!(page_id, 98765);
                assert_eq!(kind, CategoryPageKind::Articles);
                assert_eq!(page_index, 2);
                assert_eq!(total_pages, Some(10));
            }
            CallbackAction::Article { .. }
            | CallbackAction::Category { .. }
            | CallbackAction::ArticleCategory { .. }
            | CallbackAction::Wikidata { .. }
            | CallbackAction::Images { .. } => {
                panic!("expected category page callback")
            }
        }

        let action = parse_callback_data("catpage:en:98765:a:2", &[]).unwrap();
        match action {
            CallbackAction::CategoryPage { total_pages, .. } => {
                assert_eq!(total_pages, None);
            }
            _ => panic!("expected category page callback"),
        }
    }

    #[test]
    fn article_gallery_images_dedupe_filter_icons_and_cap_to_ten() {
        let infobox = MediaItem {
            title: "File:Lead.jpg".to_string(),
            url: "https://upload.wikimedia.org/lead.jpg".to_string(),
            fallback_url: None,
            caption: None,
            mime: Some("image/jpeg".to_string()),
            width: Some(1200),
            height: Some(800),
        };
        let lead_duplicate = MediaItem {
            title: "Lead.jpg".to_string(),
            url: "https://upload.wikimedia.org/lead-thumbnail.jpg".to_string(),
            fallback_url: None,
            caption: None,
            mime: Some("image/jpeg".to_string()),
            width: Some(640),
            height: Some(480),
        };
        let icon = MediaItem {
            title: "File:OOjs UI icon edit.svg".to_string(),
            url: "https://upload.wikimedia.org/icon.svg".to_string(),
            fallback_url: None,
            caption: None,
            mime: Some("image/svg+xml".to_string()),
            width: Some(20),
            height: Some(20),
        };
        let mut images = vec![lead_duplicate.clone(), icon];
        for index in 0..12 {
            images.push(MediaItem {
                title: format!("File:Photo {index}.jpg"),
                url: format!("https://upload.wikimedia.org/photo-{index}.jpg"),
                fallback_url: None,
                caption: None,
                mime: Some("image/jpeg".to_string()),
                width: Some(1000),
                height: Some(700),
            });
        }
        let media = ArticleMedia {
            lead_image: Some(lead_duplicate),
            images,
            audio: None,
        };

        let gallery = article_gallery_images(&media, Some(&infobox));

        assert_eq!(gallery.len(), ARTICLE_IMAGE_GALLERY_LIMIT);
        assert_eq!(gallery[0].title, "File:Lead.jpg");
        assert!(!gallery.iter().any(|image| image.title.contains("OOjs")));
        assert_eq!(
            gallery
                .iter()
                .filter(|image| normalized_media_title(&image.title) == "lead.jpg")
                .count(),
            1
        );
    }

    #[test]
    fn article_section_images_are_attached_to_their_section() {
        let html = r#"
            <p>Lead body.</p>
            <h2><span class="mw-headline" id="History">History</span></h2>
            <figure>
              <a href="/wiki/File:Batumi.jpg" title="File:Batumi.jpg">
                <img src="//upload.wikimedia.org/wikipedia/commons/thumb/a/a0/Batumi.jpg/220px-Batumi.jpg" width="220" height="140" alt="Batumi skyline" />
              </a>
              <figcaption><a href="/wiki/Port_of_Batumi">Port of Batumi</a> in 1881.</figcaption>
            </figure>
            <p>History body.</p>
        "#;

        let sections = mediawiki_html_to_telegram_sections("en", "Batumi", html);

        assert_eq!(sections.len(), 2);
        assert!(sections[0].images.is_empty());
        assert_eq!(sections[1].images.len(), 1);
        assert_eq!(sections[1].images[0].title, "File:Batumi.jpg");
        assert_eq!(
            sections[1].images[0].url,
            "https://upload.wikimedia.org/wikipedia/commons/a/a0/Batumi.jpg"
        );
        assert_eq!(
            sections[1].images[0].fallback_url.as_deref(),
            Some(
                "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a0/Batumi.jpg/3840px-Batumi.jpg"
            )
        );
        assert_eq!(
            sections[1].images[0].caption.as_deref(),
            Some(
                "<a href=\"https://en.wikipedia.org/wiki/Port_of_Batumi\">Port of Batumi</a> in 1881."
            )
        );
    }

    #[test]
    fn image_info_media_items_use_original_with_thumbnail_fallback() {
        let item = media_item_from_image_info_page(ImageInfoPage {
            title: "File:Large image.tif".to_string(),
            imageinfo: Some(vec![ImageInfo {
                url: "https://upload.wikimedia.org/original.tif".to_string(),
                mime: Some("image/tiff".to_string()),
                width: Some(4000),
                height: Some(3000),
                thumburl: Some("https://upload.wikimedia.org/thumb.jpg".to_string()),
            }]),
        })
        .unwrap();

        assert_eq!(item.url, "https://upload.wikimedia.org/original.tif");
        assert_eq!(
            item.fallback_url.as_deref(),
            Some("https://upload.wikimedia.org/thumb.jpg")
        );
        assert_eq!(item.width, Some(4000));
        assert_eq!(item.height, Some(3000));
    }

    #[test]
    fn wikimedia_thumbnail_urls_can_be_rewritten_to_larger_widths() {
        let thumb = "https://upload.wikimedia.org/wikipedia/commons/thumb/b/b9/1071_-_Kaukasus_2014_-_Georgien_-_Batumi_%2816728246174%29.jpg/250px-1071_-_Kaukasus_2014_-_Georgien_-_Batumi_%2816728246174%29.jpg";

        assert_eq!(
            wikimedia_original_image_url_from_thumbnail(thumb).as_deref(),
            Some(
                "https://upload.wikimedia.org/wikipedia/commons/b/b9/1071_-_Kaukasus_2014_-_Georgien_-_Batumi_%2816728246174%29.jpg"
            )
        );
        assert_eq!(
            wikimedia_thumbnail_url_with_width(thumb, ARTICLE_IMAGE_FALLBACK_THUMB_WIDTH)
                .as_deref(),
            Some(
                "https://upload.wikimedia.org/wikipedia/commons/thumb/b/b9/1071_-_Kaukasus_2014_-_Georgien_-_Batumi_%2816728246174%29.jpg/3840px-1071_-_Kaukasus_2014_-_Georgien_-_Batumi_%2816728246174%29.jpg"
            )
        );
    }

    #[test]
    fn article_gallery_filters_images_already_sent_inline() {
        let images = vec![
            MediaItem {
                title: "File:Batumi.jpg".to_string(),
                url: "https://upload.wikimedia.org/original.jpg".to_string(),
                fallback_url: None,
                caption: None,
                mime: Some("image/jpeg".to_string()),
                width: Some(1200),
                height: Some(800),
            },
            MediaItem {
                title: "File:Other.jpg".to_string(),
                url: "https://upload.wikimedia.org/other.jpg".to_string(),
                fallback_url: None,
                caption: None,
                mime: Some("image/jpeg".to_string()),
                width: Some(1200),
                height: Some(800),
            },
        ];
        let excluded = vec![MediaItem {
            title: "File:Batumi.jpg".to_string(),
            url: "https://upload.wikimedia.org/thumb.jpg".to_string(),
            fallback_url: None,
            caption: None,
            mime: None,
            width: Some(220),
            height: Some(140),
        }];

        let filtered = filter_excluded_media_items(images, &excluded);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].title, "File:Other.jpg");
    }

    #[test]
    fn article_images_keyboard_uses_image_callback() {
        let keyboard = article_images_keyboard("en", 42, 12).unwrap();
        let button = &keyboard["inline_keyboard"][0][0];

        assert_eq!(button["text"], "Images (10)");
        assert_eq!(button["callback_data"], "images:en:42");
    }

    #[test]
    fn parses_category_aliases() {
        for text in [
            "Category:Physics",
            "Category: Physics",
            "Category Physics",
            "c Physics",
        ] {
            assert_eq!(
                parse_category_command(text, "en").unwrap(),
                Some(CategoryRequest {
                    language: "en".to_string(),
                    title: "Category:Physics".to_string()
                })
            );
        }
        assert_eq!(
            parse_category_command("Категория Физика", "en").unwrap(),
            Some(CategoryRequest {
                language: "ru".to_string(),
                title: "Категория:Физика".to_string()
            })
        );
        assert_eq!(
            parse_category_command("Категория:Физика", "en").unwrap(),
            Some(CategoryRequest {
                language: "ru".to_string(),
                title: "Категория:Физика".to_string()
            })
        );
        assert_eq!(
            parse_category_command("к Физика", "en").unwrap(),
            Some(CategoryRequest {
                language: "ru".to_string(),
                title: "Категория:Физика".to_string()
            })
        );
        assert_eq!(
            parse_category_command("Катэгорыя Навука", "en").unwrap(),
            Some(CategoryRequest {
                language: "be".to_string(),
                title: "Катэгорыя:Навука".to_string()
            })
        );
        assert_eq!(
            parse_category_command("к Фізіка", "en").unwrap(),
            Some(CategoryRequest {
                language: "be".to_string(),
                title: "Катэгорыя:Фізіка".to_string()
            })
        );
    }

    #[test]
    fn category_title_inclusion_requires_all_query_terms() {
        assert!(category_title_matches_query(
            "Category:Video games about religion",
            "games religion"
        ));
        assert!(!category_title_matches_query(
            "Category:Video games about cults",
            "games religion"
        ));
    }

    #[test]
    fn category_search_score_prefers_broad_categories() {
        let mut categories = [
            (1_usize, "Category:Video games about religion"),
            (2_usize, "Category:Games"),
            (3_usize, "Category:Video games"),
            (4_usize, "Category:Wikipedia requested images of games"),
        ];

        categories.sort_by_key(|(rank, title)| category_search_score(title, "games", *rank));

        assert_eq!(categories[0].1, "Category:Games");
        assert_eq!(categories[1].1, "Category:Video games");
        assert_eq!(
            categories[3].1,
            "Category:Wikipedia requested images of games"
        );
    }

    #[test]
    fn category_search_message_has_clickable_names_and_buttons() {
        let categories = vec![Category {
            title: "Category:Video games about religion".to_string(),
            article_count: None,
        }];

        let rendered = render_category_search_html("en", "games religion", &categories);
        assert!(rendered.contains("<b>Categories matching games religion</b>"));
        assert!(rendered.contains(
            "<a href=\"https://en.wikipedia.org/wiki/Category%3AVideo_games_about_religion\">Video games about religion</a>"
        ));

        let keyboard = category_search_keyboard("en", &categories).unwrap();
        let keyboard = serde_json::to_string(&keyboard).unwrap();
        assert!(keyboard.contains("Video games about religion"));
        assert!(keyboard.contains("category:en:"));
    }

    #[test]
    fn category_members_response_deserializes_continue_and_empty_members() {
        let response: CategoryMembersResponse = serde_json::from_str(
            r#"{
                "batchcomplete": true,
                "continue": {
                    "cmcontinue": "20250617074824|54505381",
                    "continue": "-||"
                },
                "query": {
                    "categorymembers": []
                }
            }"#,
        )
        .unwrap();

        assert_eq!(response.query.unwrap().categorymembers.len(), 0);
        assert_eq!(
            response.continuation.unwrap().cmcontinue.as_deref(),
            Some("20250617074824|54505381")
        );
    }

    #[test]
    fn category_headings_link_full_category_title() {
        let rendered =
            render_category_subcategories_page_heading_html("en", "Category:PC games", 0, None);

        assert!(rendered.starts_with("Subcategories in "));
        assert!(rendered.contains(
            "<a href=\"https://en.wikipedia.org/wiki/Category%3APC_games\">Category:PC games</a>"
        ));

        let rendered =
            render_category_articles_page_heading_html("en", "Category:PC games", 0, None);
        assert!(rendered.starts_with("Newest article pages in "));
        assert!(rendered.contains(
            "<a href=\"https://en.wikipedia.org/wiki/Category%3APC_games\">Category:PC games</a>"
        ));

        let rendered =
            render_category_articles_page_heading_html("en", "Category:PC games", 1, None);
        assert!(rendered.ends_with("(page 2)"));

        let rendered =
            render_category_articles_page_heading_html("en", "Category:PC games", 4, Some(10));
        assert!(rendered.ends_with("(page 5/10)"));
    }

    #[test]
    fn category_article_keyboard_adds_next_and_previous_buttons() {
        let keyboard = category_articles_keyboard(
            "en",
            &[ArticleButton {
                page_id: 42,
                title: "Example".to_string(),
                size: Some(2000),
                wordcount: None,
                snippet: None,
                is_disambiguation: false,
            }],
            DEFAULT_BIG_ARTICLE_CHAR_THRESHOLD,
            DEFAULT_SMALL_ARTICLE_CHAR_THRESHOLD,
            false,
            Some(CategoryPagination {
                language: "en",
                page_id: 98765,
                kind: CategoryPageKind::Articles,
                page_index: 1,
                total_pages: Some(10),
                has_next: true,
            }),
        )
        .unwrap();

        let rows = keyboard["inline_keyboard"].as_array().unwrap();
        let nav = rows.last().unwrap().as_array().unwrap();
        assert_eq!(nav[0]["text"], "Prev");
        assert_eq!(nav[0]["callback_data"], "catpage:en:98765:a:0:10");
        assert_eq!(nav[1]["text"], "Next");
        assert_eq!(nav[1]["callback_data"], "catpage:en:98765:a:2:10");
    }

    #[test]
    fn category_subcategory_keyboard_adds_next_button_on_first_page() {
        let keyboard = category_subcategories_keyboard_with_pagination(
            "en",
            &[Category {
                title: "Category:PC games by genre".to_string(),
                article_count: None,
            }],
            Some(CategoryPagination {
                language: "en",
                page_id: 98765,
                kind: CategoryPageKind::Subcategories,
                page_index: 0,
                total_pages: Some(3),
                has_next: true,
            }),
        )
        .unwrap();

        let rows = keyboard["inline_keyboard"].as_array().unwrap();
        let nav = rows.last().unwrap().as_array().unwrap();
        assert_eq!(nav.len(), 1);
        assert_eq!(nav[0]["text"], "Next");
        assert_eq!(nav[0]["callback_data"], "catpage:en:98765:s:1:3");
    }

    #[test]
    fn single_subcategory_button_uses_primary_style() {
        let keyboard = category_subcategories_keyboard_with_pagination(
            "en",
            &[Category {
                title: "Category:PC games by genre".to_string(),
                article_count: None,
            }],
            None,
        )
        .unwrap();

        let rows = keyboard["inline_keyboard"].as_array().unwrap();
        assert!(
            rows[0][0]["callback_data"]
                .as_str()
                .unwrap()
                .starts_with("category:en:")
        );
        assert_eq!(rows[0][0]["style"], "primary");
    }

    #[test]
    fn many_subcategory_buttons_use_danger_style() {
        let subcategories = (0..11)
            .map(|index| Category {
                title: format!("Category:PC games subgroup {index}"),
                article_count: None,
            })
            .collect::<Vec<_>>();

        let keyboard =
            category_subcategories_keyboard_with_pagination("en", &subcategories, None).unwrap();

        let rows = keyboard["inline_keyboard"].as_array().unwrap();
        assert_eq!(rows[0][0]["style"], "danger");
    }

    #[test]
    fn article_category_buttons_use_article_count_styles() {
        let keyboard = article_categories_keyboard(
            "en",
            42,
            &[
                Category {
                    title: "Category:One article".to_string(),
                    article_count: Some(1),
                },
                Category {
                    title: "Category:Many articles".to_string(),
                    article_count: Some(11),
                },
                Category {
                    title: "Category:Ten articles".to_string(),
                    article_count: Some(10),
                },
                Category {
                    title: "Category:Unknown articles".to_string(),
                    article_count: None,
                },
            ],
        )
        .unwrap();

        let rows = keyboard["inline_keyboard"].as_array().unwrap();
        assert_eq!(rows[0][0]["style"], "primary");
        assert_eq!(rows[0][1]["style"], "danger");
        assert!(rows[1][0].get("style").is_none());
        assert!(rows[1][1].get("style").is_none());
    }

    #[test]
    fn wikidata_metadata_button_uses_property_count_styles() {
        assert_eq!(wikidata_property_button_style(Some(4)), Some("primary"));
        assert_eq!(wikidata_property_button_style(Some(5)), None);
        assert_eq!(wikidata_property_button_style(Some(10)), None);
        assert_eq!(wikidata_property_button_style(Some(11)), Some("danger"));
        assert_eq!(wikidata_property_button_style(None), None);
    }

    #[test]
    fn category_description_renders_clickable_heading() {
        let rendered = render_category_description_html_parts(
            "en",
            "Category:Linux games",
            "Articles about Linux games.",
            3900,
        )
        .join("\n\n");

        assert!(rendered.contains(
            "<b><a href=\"https://en.wikipedia.org/wiki/Category%3ALinux_games\">Category:Linux games</a></b>"
        ));
        assert!(rendered.contains("Articles about Linux games."));
    }

    #[test]
    fn large_result_buttons_use_danger_style() {
        let keyboard = article_results_keyboard(
            "en",
            &[ArticleButton {
                page_id: 42,
                title: "Large Article".to_string(),
                size: Some(DEFAULT_BIG_ARTICLE_CHAR_THRESHOLD),
                wordcount: Some(9000),
                snippet: None,
                is_disambiguation: false,
            }],
            DEFAULT_BIG_ARTICLE_CHAR_THRESHOLD,
            DEFAULT_SMALL_ARTICLE_CHAR_THRESHOLD,
            false,
        );

        assert_eq!(keyboard[0][0]["callback_data"], "article:en:42");
        assert_eq!(keyboard[0][0]["style"], "danger");
    }

    #[test]
    fn small_result_buttons_use_primary_style() {
        let keyboard = article_results_keyboard(
            "en",
            &[ArticleButton {
                page_id: 43,
                title: "Small Article".to_string(),
                size: Some(DEFAULT_SMALL_ARTICLE_CHAR_THRESHOLD),
                wordcount: Some(100),
                snippet: None,
                is_disambiguation: false,
            }],
            DEFAULT_BIG_ARTICLE_CHAR_THRESHOLD,
            DEFAULT_SMALL_ARTICLE_CHAR_THRESHOLD,
            false,
        );

        assert_eq!(keyboard[0][0]["callback_data"], "article:en:43");
        assert_eq!(keyboard[0][0]["style"], "primary");
    }

    #[test]
    fn disambiguation_result_buttons_use_emoji_marker() {
        let keyboard = article_results_keyboard(
            "en",
            &[ArticleButton {
                page_id: 45,
                title: "Kitty".to_string(),
                size: Some(1200),
                wordcount: None,
                snippet: None,
                is_disambiguation: true,
            }],
            DEFAULT_BIG_ARTICLE_CHAR_THRESHOLD,
            DEFAULT_SMALL_ARTICLE_CHAR_THRESHOLD,
            false,
        );

        assert_eq!(keyboard[0][0]["text"], "🔀 Kitty");
        assert_eq!(keyboard[0][0]["callback_data"], "article:en:45");
    }

    #[test]
    fn single_category_article_button_uses_primary_style() {
        let keyboard = article_results_keyboard(
            "en",
            &[ArticleButton {
                page_id: 44,
                title: "Only Article".to_string(),
                size: Some(10_000),
                wordcount: None,
                snippet: None,
                is_disambiguation: false,
            }],
            DEFAULT_BIG_ARTICLE_CHAR_THRESHOLD * 10,
            DEFAULT_SMALL_ARTICLE_CHAR_THRESHOLD,
            true,
        );

        assert_eq!(keyboard[0][0]["style"], "primary");
    }

    #[test]
    fn splits_text_under_configured_limit() {
        let text = "alpha beta gamma delta epsilon zeta eta theta";
        let parts = split_telegram_text(text, 12);

        assert!(parts.len() > 1);
        assert!(parts.iter().all(|part| part.chars().count() <= 12));
        assert_eq!(parts.join(" "), text);
    }

    #[test]
    fn extracts_infobox_text_from_html_table() {
        let html = r#"
            <p>Intro</p>
            <table class="infobox vcard">
              <style>.mw-parser-output .locmap .od{position:absolute}</style>
              <tr><td><a href="/wiki/File:Flag.svg" title="File:Flag.svg"><img src="//upload.wikimedia.org/wikipedia/commons/thumb/a/a1/Flag.svg/320px-Flag.svg.png" alt="Flag" /></a></td></tr>
              <tr><th>Born</th><td>1 January 1900<sup>1</sup></td></tr>
              <tr><th>Developer</th><td><a href="/wiki/Black_Hole_Entertainment">Black Hole Entertainment</a></td></tr>
              <tr><th>Location</th></tr>
              <tr><td>Interactive map of the Example area</td></tr>
              <tr><td>Show map of North Carolina Show map of the United States</td></tr>
              <tr><th>Стартовая площадка</th><td>45°55′ с. ш. 63°20′ в. д.</td></tr>
              <tr><td>Медиафайлы на Викискладе</td></tr>
              <tr><td>1</td></tr>
              <tr><td>«Кедр»</td></tr>
              <tr><th>Occupation</th><td>Writer<br />Teacher</td></tr>
            </table>
            <p>Article body</p>
        "#;

        let infobox = extract_infobox_text("en", html).unwrap();
        assert!(infobox.contains("Born"));
        assert!(infobox.contains("1 January 1900"));
        assert!(infobox.contains(
            "Developer: <a href=\"https://en.wikipedia.org/wiki/Black_Hole_Entertainment\">Black Hole Entertainment</a>"
        ));
        assert!(infobox.contains("Occupation"));
        assert!(infobox.contains("Teacher"));
        assert!(infobox.contains("Стартовая площадка: 45°55′ с. ш. 63°20′ в. д."));
        assert!(!infobox.contains("Location :"));
        assert!(!infobox.contains("Interactive map"));
        assert!(!infobox.contains("locmap"));
        assert!(!infobox.contains("Show map"));
        assert!(!infobox.contains("Медиафайлы"));
        assert!(!infobox.lines().any(|line| line == "1"));
        assert!(!infobox.contains("«Кедр»"));
        assert!(!infobox.contains("Article body"));

        let rendered = render_infobox_html(&infobox, 3900);
        assert!(rendered.contains(
            "https://geohack.toolforge.org/geohack.php?params=45.916666666666664_N_63.333333333333336_E"
        ));

        let image = extract_infobox_image(html).unwrap();
        assert_eq!(image.title, "File:Flag.svg");
        assert_eq!(
            image.url,
            "https://upload.wikimedia.org/wikipedia/commons/a/a1/Flag.svg"
        );
        assert_eq!(
            image.fallback_url.as_deref(),
            Some(
                "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a1/Flag.svg/3840px-Flag.svg.png"
            )
        );
    }

    #[test]
    fn article_html_has_bold_headers_and_normal_links() {
        let article = ArticleContent {
            title: "Example & Test".to_string(),
            extract: String::new(),
            length: Some(1000),
            infobox: Some("Kind: Example".to_string()),
            infobox_image: None,
            body_html: Some(
                "A <a href=\"https://en.wikipedia.org/wiki/Linked_Article\">Linked Article</a> body."
                    .to_string(),
            ),
            body_sections: Vec::new(),
            references_html: None,
            body_links: Vec::new(),
            nav_templates: Vec::new(),
            is_disambiguation: false,
            disambiguation_groups: Vec::new(),
        };
        let rendered = render_article_html_parts("en", &article, 3900).join("\n\n");

        assert!(rendered.contains(
            "<b><a href=\"https://en.wikipedia.org/wiki/Example_%26_Test\">Example &amp; Test</a></b>"
        ));
        assert!(!rendered.contains("Open on Wikipedia"));
        assert!(rendered.contains("<b>Infobox</b>"));
        assert!(rendered.contains("Linked Article</a> body."));
        assert!(rendered.contains(
            "<a href=\"https://en.wikipedia.org/wiki/Linked_Article\">Linked Article</a>"
        ));
    }

    #[test]
    fn article_html_splitting_does_not_break_links() {
        let linked_line = format!(
            "Start {} <a href=\"https://en.wikipedia.org/wiki/Linked_Article\">Linked Article</a> end.",
            "word ".repeat(4)
        );
        let article = ArticleContent {
            title: "Example".to_string(),
            extract: String::new(),
            length: Some(1000),
            infobox: None,
            infobox_image: None,
            body_html: Some([linked_line.as_str(); 10].join("\n")),
            body_sections: Vec::new(),
            references_html: None,
            body_links: Vec::new(),
            nav_templates: Vec::new(),
            is_disambiguation: false,
            disambiguation_groups: Vec::new(),
        };

        let parts = render_article_html_parts("en", &article, 650);

        assert!(parts.len() > 1);
        for part in parts {
            assert_eq!(part.matches("<a ").count(), part.matches("</a>").count());
        }
    }

    #[test]
    fn article_body_sections_split_with_clickable_article_and_italic_section_prefixes() {
        let article = ArticleContent {
            title: "Example Article".to_string(),
            extract: String::new(),
            length: Some(1000),
            infobox: None,
            infobox_image: None,
            body_html: None,
            body_sections: vec![
                ArticleBodySection {
                    heading: None,
                    html: "Lead body.".to_string(),
                    images: Vec::new(),
                },
                ArticleBodySection {
                    heading: Some(ArticleBodyHeading {
                        title: "History".to_string(),
                        anchor: Some("History".to_string()),
                    }),
                    html: "History body.".to_string(),
                    images: Vec::new(),
                },
            ],
            references_html: None,
            body_links: Vec::new(),
            nav_templates: Vec::new(),
            is_disambiguation: false,
            disambiguation_groups: Vec::new(),
        };

        let parts = render_article_html_parts("en", &article, 320);

        assert!(parts.len() >= 3);
        assert!(parts.iter().any(|part| part.starts_with(
            "<a href=\"https://en.wikipedia.org/wiki/Example_Article\">Example Article</a>\n\nLead body."
        )));
        assert!(!parts.iter().any(|part| part.contains(">Lead</a>")));
        assert!(parts.iter().any(|part| part.starts_with(
            "<a href=\"https://en.wikipedia.org/wiki/Example_Article\">Example Article</a>: <i><a href=\"https://en.wikipedia.org/wiki/Example_Article#History\">History</a></i>"
        )));
    }

    #[test]
    fn mediawiki_html_to_telegram_sections_preserves_heading_anchors() {
        let html = r#"
            <p>Lead body.</p>
            <h2><span class="mw-headline" id="History">History</span></h2>
            <p>History body.</p>
            <h2>References</h2>
            <ol class="references"><li>Reference body.</li></ol>
        "#;

        let sections = mediawiki_html_to_telegram_sections("en", "Example Article", html);

        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].heading, None);
        assert_eq!(
            sections[1].heading,
            Some(ArticleBodyHeading {
                title: "History".to_string(),
                anchor: Some("History".to_string()),
            })
        );
        assert!(sections[1].html.contains("History body."));
        assert!(
            !sections
                .iter()
                .any(|section| section.html.contains("Reference body"))
        );
    }

    #[test]
    fn mediawiki_html_skips_climate_data_tables_but_keeps_regular_tables() {
        let html = r#"
            <p>Lead body.</p>
            <table class="wikitable">
              <tbody>
                <tr><th>Name</th><th>Use</th></tr>
                <tr><td><a href="/wiki/Batumi_Tower">Batumi Tower</a></td><td>Skyscraper</td></tr>
              </tbody>
            </table>
            <table class="wikitable mw-collapsible">
              <tbody>
                <tr><th colspan="14">Climate data for Batumi Airport (normals for 1981-2010)</th></tr>
                <tr><th>Month</th><th>Jan</th><th>Feb</th><th>Year</th></tr>
                <tr><th>Record high °C (°F)</th><td>25.3</td><td>27.4</td><td>40.8</td></tr>
                <tr><th>Daily mean °C (°F)</th><td>5.4</td><td>6.8</td><td>14.2</td></tr>
              </tbody>
            </table>
        "#;

        let rendered = mediawiki_html_to_telegram_html("en", "Batumi", html).unwrap();

        assert!(rendered.contains("<pre>Name         | Use\nBatumi Tower | Skyscraper</pre>"));
        assert!(rendered.contains("<b>Table links</b>"));
        assert!(
            rendered.contains(
                "<a href=\"https://en.wikipedia.org/wiki/Batumi_Tower\">Batumi Tower</a>"
            )
        );
        assert!(!rendered.contains("Climate data for Batumi Airport"));
        assert!(!rendered.contains("Record high"));
        assert!(!rendered.contains("Daily mean"));
    }

    #[test]
    fn mediawiki_html_formats_tables_as_aligned_monospace_rows() {
        let html = r#"
            <table class="wikitable">
              <tbody>
                <tr><th>Year</th><th colspan="2">Georgians</th><th colspan="2">Armenians</th><th>Total</th></tr>
                <tr><td>1886</td><td>2,518</td><td>17%</td><td>3,458</td><td>23.4%</td><td>14,803</td></tr>
                <tr><td>2014</td><td>142,691</td><td>93.4%</td><td>4,636</td><td>3.0%</td><td>152,839</td></tr>
              </tbody>
            </table>
        "#;

        let rendered = mediawiki_html_to_telegram_html("en", "Batumi", html).unwrap();

        assert!(rendered.contains(
            "<pre>Year | Georgians |       | Armenians |       | Total\n1886 | 2,518     | 17%   | 3,458     | 23.4% | 14,803\n2014 | 142,691   | 93.4% | 4,636     | 3.0%  | 152,839</pre>"
        ));
    }

    #[test]
    fn mediawiki_html_lists_all_distinct_links_after_tables() {
        let html = r#"
            <table class="wikitable">
              <tbody>
                <tr><th>Name</th><th>Location</th></tr>
                <tr>
                  <td><a href="/wiki/Batumi_Tower">Batumi Tower</a></td>
                  <td><a href="/wiki/Batumi">Batumi</a></td>
                </tr>
                <tr>
                  <td><a href="/wiki/Batumi_Tower">Batumi Tower duplicate</a></td>
                  <td><a href="/wiki/Adjara">Adjara</a></td>
                </tr>
              </tbody>
            </table>
        "#;

        let rendered = mediawiki_html_to_telegram_html("en", "Batumi", html).unwrap();

        assert!(rendered.contains("<b>Table links</b>"));
        assert_eq!(
            rendered
                .matches("https://en.wikipedia.org/wiki/Batumi_Tower")
                .count(),
            1
        );
        assert!(rendered.contains("<a href=\"https://en.wikipedia.org/wiki/Batumi\">Batumi</a>"));
        assert!(rendered.contains("<a href=\"https://en.wikipedia.org/wiki/Adjara\">Adjara</a>"));
    }

    #[test]
    fn mediawiki_html_preserves_inline_body_links_and_skips_refs() {
        let html = r#"
            <p><b>Rust</b> is a <a href="/wiki/Programming_language">programming language</a>.<sup class="reference">[1]</sup></p>
            <p>Use <code>cargo test</code>.</p>
            <blockquote>Quoted <i>text</i>.</blockquote>
            <h2><span class="mw-headline" id="History">History</span></h2>
            <p>History body.</p>
            <table class="wikitable">
              <tbody>
                <tr><th>Name</th><th>Use</th></tr>
                <tr><td><a href="/wiki/Batumi_Tower">Batumi Tower</a></td><td>Skyscraper<br />hotel</td></tr>
              </tbody>
            </table>
            <h2>References</h2>
            <ol class="references"><li>Reference text</li></ol>
        "#;

        let rendered = mediawiki_html_to_telegram_html("en", "Rust & Test", html).unwrap();

        assert!(rendered.contains("<b>Rust</b>"));
        assert!(rendered.contains(
            "<a href=\"https://en.wikipedia.org/wiki/Programming_language\">programming language</a>"
        ));
        assert!(rendered.contains("<code>cargo test</code>"));
        assert!(rendered.contains("<blockquote>Quoted <i>text</i>.</blockquote>"));
        assert!(rendered.contains(
            "<b><a href=\"https://en.wikipedia.org/wiki/Rust_%26_Test#History\">History</a></b>"
        ));
        assert!(
            rendered.contains("<pre>Name         | Use\nBatumi Tower | Skyscraper / hotel</pre>")
        );
        assert!(rendered.contains("<b>Table links</b>"));
        assert!(
            rendered.contains(
                "<a href=\"https://en.wikipedia.org/wiki/Batumi_Tower\">Batumi Tower</a>"
            )
        );
        assert!(!rendered.contains("[1]"));
        assert!(!rendered.contains("Reference text"));
    }

    #[test]
    fn mediawiki_body_links_extracts_first_internal_body_links() {
        let html = r#"
            <table class="infobox"><tr><td><a href="/wiki/Infobox_Target">Infobox target</a></td></tr></table>
            <p><b>Example</b> links to <a href="/wiki/First_Target">first target</a>,
               <a href="/wiki/Second_Target">second target</a>,
               <a href="/wiki/First_Target">duplicate first target</a>,
               <a href="/wiki/Example">self target</a>,
               <a class="new" href="/wiki/Missing_Target">missing target</a>,
               and <a href="/wiki/Help:Contents">namespaced target</a>.</p>
            <div class="navbox"><a href="/wiki/Nav_Target">Nav target</a></div>
            <ol class="references"><li><a href="/wiki/Reference_Target">Reference target</a></li></ol>
        "#;

        let links = mediawiki_body_links("en", "Example", html);

        assert_eq!(
            links,
            vec![
                NavLink {
                    label: "first target".to_string(),
                    page_title: "First Target".to_string(),
                },
                NavLink {
                    label: "second target".to_string(),
                    page_title: "Second Target".to_string(),
                },
            ]
        );
    }

    #[test]
    fn golden_armies_of_exigo_keeps_infobox_body_and_reference_links() {
        let infobox = extract_infobox_text("en", ARMIES_OF_EXIGO_HTML).unwrap();

        assert!(infobox.contains("Armies of Exigo"));
        assert!(infobox.contains(
            "Developer: <a href=\"https://en.wikipedia.org/wiki/Black_Hole_Entertainment\">Black Hole Entertainment</a>"
        ));
        assert!(infobox.contains(
            "Platform: <a href=\"https://en.wikipedia.org/wiki/Microsoft_Windows\">Microsoft Windows</a>"
        ));
        assert!(infobox.contains(
            "Modes: <a href=\"https://en.wikipedia.org/wiki/Single-player_video_game\">Single-player</a>, <a href=\"https://en.wikipedia.org/wiki/Multiplayer_video_game\">multiplayer</a>"
        ));

        let body =
            mediawiki_html_to_telegram_html("en", "Armies of Exigo", ARMIES_OF_EXIGO_HTML).unwrap();
        assert!(body.contains(
            "<a href=\"https://en.wikipedia.org/wiki/Real-time_strategy\">real-time strategy</a>"
        ));
        assert!(body.contains(
            "<a href=\"https://en.wikipedia.org/wiki/Black_Hole_Entertainment\">Black Hole Entertainment</a>"
        ));
        assert!(!body.contains("Example Review"));

        let references = mediawiki_references_to_telegram_html("en", ARMIES_OF_EXIGO_HTML).unwrap();
        assert!(references.contains("1. Review at Example Review."));
        assert!(references.contains(
            "<a href=\"https://example.com/armies-review\">https://example.com/armies-review</a>"
        ));
        assert!(!references.contains("^"));
    }

    #[test]
    fn golden_mill_farm_inn_drops_locmap_css_and_keeps_real_fields() {
        let infobox = extract_infobox_text("en", MILL_FARM_INN_HTML).unwrap();

        assert!(infobox.contains("Mill Farm Inn"));
        assert!(infobox.contains("Mill Farm Inn, September 2012"));
        assert!(infobox.contains(
            "Location: <a href=\"https://en.wikipedia.org/wiki/Anson_County,_North_Carolina\">Anson County, North Carolina</a>"
        ));
        assert!(infobox.contains("Built: 1840"));
        assert!(!infobox.contains(".mw-parser-output"));
        assert!(!infobox.contains("position:absolute"));
        assert!(!infobox.contains("locmap"));
        assert!(!infobox.contains("Show map"));

        let image = extract_infobox_image(MILL_FARM_INN_HTML).unwrap();
        assert_eq!(image.title, "File:Mill Farm Inn.jpg");
        assert_eq!(
            image.url,
            "https://upload.wikimedia.org/wikipedia/commons/0/0a/Mill_Farm_Inn.jpg"
        );
        assert_eq!(
            image.fallback_url.as_deref(),
            Some(
                "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/Mill_Farm_Inn.jpg/3840px-Mill_Farm_Inn.jpg"
            )
        );
    }

    #[test]
    fn golden_porta_batumi_tower_drops_map_caption_and_keeps_navigation_title() {
        let infobox = extract_infobox_text("en", PORTA_BATUMI_TOWER_HTML).unwrap();
        assert!(infobox.contains("Porta Batumi Tower"));
        assert!(infobox.contains("Status: Completed"));
        assert!(infobox.contains(
            "Location: <a href=\"https://en.wikipedia.org/wiki/Batumi\">Batumi</a>, <a href=\"https://en.wikipedia.org/wiki/Georgia_(country)\">Georgia</a>"
        ));
        assert!(!infobox.contains("Interactive map"));

        let body =
            mediawiki_html_to_telegram_html("en", "Porta Batumi Tower", PORTA_BATUMI_TOWER_HTML)
                .unwrap();
        assert!(body.contains("<a href=\"https://en.wikipedia.org/wiki/Batumi\">Batumi</a>"));

        let templates = mediawiki_nav_templates("en", PORTA_BATUMI_TOWER_HTML);
        assert_eq!(templates.len(), 1);
        assert_eq!(templates[0].title, "Skyscrapers in Georgia");
        assert_eq!(
            templates[0].title_url.as_deref(),
            Some("https://en.wikipedia.org/wiki/List_of_tallest_buildings_in_Georgia_(country)")
        );
        assert!(
            templates[0]
                .links
                .iter()
                .any(|link| link.label == "Batumi Tower" && link.page_title == "Batumi Tower")
        );
    }

    #[test]
    fn golden_vostok_1_ru_links_coordinates_and_drops_infobox_noise() {
        let infobox = extract_infobox_text("ru", VOSTOK_1_RU_HTML).unwrap();

        assert!(infobox.contains("Восток-1"));
        assert!(
            infobox.contains("Страна: <a href=\"https://ru.wikipedia.org/wiki/СССР\">СССР</a>")
        );
        assert!(infobox.contains("Стартовая площадка: 45°55′ с. ш. 63°20′ в. д."));
        assert!(!infobox.contains("Медиафайлы"));
        assert!(!infobox.lines().any(|line| line == "1"));
        assert!(!infobox.contains("«Кедр»"));

        let rendered = render_infobox_html(&infobox, 3900);
        assert!(rendered.contains(
            "https://geohack.toolforge.org/geohack.php?params=45.916666666666664_N_63.333333333333336_E"
        ));

        let body = mediawiki_html_to_telegram_html("ru", "Восток-1", VOSTOK_1_RU_HTML).unwrap();
        assert!(body.contains(
            "<a href=\"https://ru.wikipedia.org/wiki/Гагарин,_Юрий_Алексеевич\">Юрий Гагарин</a>"
        ));
    }

    #[test]
    fn golden_flag_of_adjara_extracts_image_and_clickable_navigation_title() {
        let image = extract_infobox_image(FLAG_OF_ADJARA_HTML).unwrap();
        assert_eq!(image.title, "File:Flag of Adjara.svg");
        assert_eq!(
            image.url,
            "https://upload.wikimedia.org/wikipedia/commons/7/71/Flag_of_Adjara.svg"
        );
        assert_eq!(
            image.fallback_url.as_deref(),
            Some(
                "https://upload.wikimedia.org/wikipedia/commons/thumb/7/71/Flag_of_Adjara.svg/3840px-Flag_of_Adjara.svg.png"
            )
        );

        let templates = mediawiki_nav_templates("en", FLAG_OF_ADJARA_HTML);
        assert_eq!(templates.len(), 1);
        assert_eq!(
            templates[0].title,
            "Flags of the Autonomous Soviet Socialist Republics"
        );
        assert_eq!(
            templates[0].title_url.as_deref(),
            Some(
                "https://en.wikipedia.org/wiki/Flags_of_the_Autonomous_Soviet_Socialist_Republics"
            )
        );

        let heading = render_navigation_heading_html(&NavTemplateButtons {
            title: templates[0].title.clone(),
            title_url: templates[0].title_url.clone(),
            buttons: Vec::new(),
        });
        assert_eq!(
            heading,
            "<b>Navigation: <a href=\"https://en.wikipedia.org/wiki/Flags_of_the_Autonomous_Soviet_Socialist_Republics\">Flags of the Autonomous Soviet Socialist Republics</a></b>"
        );
    }

    #[test]
    fn disambiguation_pages_render_target_buttons_instead_of_body() {
        let parsed = ParsedPage {
            title: Some("Mercury".to_string()),
            text: None,
            properties: Some(HashMap::from([(
                "disambiguation".to_string(),
                Value::String(String::new()),
            )])),
        };
        let html = r#"
            <p><b>Mercury</b> may refer to:</p>
            <ul>
              <li><a href="/wiki/Mercury_(planet)">Mercury (planet)</a>, the closest planet to the Sun</li>
              <li><a href="/wiki/Mercury_(element)">Mercury (element)</a>, a chemical element</li>
              <li><a class="mw-disambig" href="/wiki/Mercury_Theatre_(disambiguation)">Mercury Theatre</a></li>
              <li><a href="/wiki/Category:Mercury">Category</a></li>
            </ul>
            <div id="disambigbox" class="metadata dmbox-disambig">Disambiguation help text</div>
        "#;

        assert!(is_disambiguation_page(&parsed, html));
        assert_eq!(
            mediawiki_disambiguation_link_groups("en", html),
            vec![DisambiguationLinkGroup {
                title: "Other uses".to_string(),
                links: vec![
                    NavLink {
                        label: "Mercury (planet)".to_string(),
                        page_title: "Mercury (planet)".to_string(),
                    },
                    NavLink {
                        label: "Mercury (element)".to_string(),
                        page_title: "Mercury (element)".to_string(),
                    },
                ],
            }]
        );

        let article = ArticleContent {
            title: "Mercury".to_string(),
            extract: "large disambiguation body".to_string(),
            length: Some(9999),
            infobox: None,
            infobox_image: None,
            body_html: Some("This body must not be dumped.".to_string()),
            body_sections: Vec::new(),
            references_html: None,
            body_links: Vec::new(),
            nav_templates: Vec::new(),
            is_disambiguation: true,
            disambiguation_groups: mediawiki_disambiguation_link_groups("en", html),
        };
        let rendered = render_article_html_parts("en", &article, 3900).join("\n\n");
        assert!(rendered.contains("Disambiguation page"));
        assert!(rendered.contains("Choose a target article"));
        assert!(!rendered.contains("This body must not be dumped"));
    }

    #[test]
    fn disambiguation_links_are_grouped_by_section_heading() {
        let html = r#"
            <p><b>Kitty</b> may refer to:</p>
            <h2><span id="Animals">Animals</span></h2>
            <ul>
              <li><a href="/wiki/Cat">Cat</a>, a small domesticated mammal
                <ul>
                  <li><a href="/wiki/Kitten">Kitten</a>, a young cat</li>
                </ul>
              </li>
            </ul>
            <h2><span id="Film">Film</span></h2>
            <ul>
              <li><a href="/wiki/Kitty_Films">Kitty Films</a>, an anime production company</li>
            </ul>
            <h2><span id="Games_and_money">Games and money</span></h2>
            <ul>
              <li>Kitty, <a href="/wiki/Glossary_of_poker_terms">in poker terminology</a>, a pool of money</li>
            </ul>
            <h2><span id="Music">Music</span></h2>
            <ul>
              <li><a href="/wiki/Kitty_(musician)">Kitty (musician)</a></li>
            </ul>
            <h2><span id="Other_uses">Other uses</span></h2>
            <ul>
              <li><a href="/wiki/Kitty_(given_name)">Kitty (given name)</a></li>
            </ul>
            <h2><span id="See_also">See also</span></h2>
            <ul>
              <li><a href="/wiki/Kittie">Kittie</a></li>
              <li><a class="mw-disambig" href="/wiki/Hello_Kitty_(disambiguation)">Hello Kitty</a></li>
              <li><a href="/wiki/Category:Disambiguation_pages">Category</a></li>
            </ul>
        "#;

        let groups = mediawiki_disambiguation_link_groups("en", html);

        assert_eq!(
            groups,
            vec![
                DisambiguationLinkGroup {
                    title: "Animals".to_string(),
                    links: vec![
                        NavLink {
                            label: "Cat".to_string(),
                            page_title: "Cat".to_string(),
                        },
                        NavLink {
                            label: "Kitten".to_string(),
                            page_title: "Kitten".to_string(),
                        },
                    ],
                },
                DisambiguationLinkGroup {
                    title: "Film".to_string(),
                    links: vec![NavLink {
                        label: "Kitty Films".to_string(),
                        page_title: "Kitty Films".to_string(),
                    }],
                },
                DisambiguationLinkGroup {
                    title: "Games and money".to_string(),
                    links: vec![NavLink {
                        label: "in poker terminology".to_string(),
                        page_title: "Glossary of poker terms".to_string(),
                    }],
                },
                DisambiguationLinkGroup {
                    title: "Music".to_string(),
                    links: vec![NavLink {
                        label: "Kitty (musician)".to_string(),
                        page_title: "Kitty (musician)".to_string(),
                    }],
                },
                DisambiguationLinkGroup {
                    title: "Other uses".to_string(),
                    links: vec![NavLink {
                        label: "Kitty (given name)".to_string(),
                        page_title: "Kitty (given name)".to_string(),
                    }],
                },
                DisambiguationLinkGroup {
                    title: "See also".to_string(),
                    links: vec![NavLink {
                        label: "Kittie".to_string(),
                        page_title: "Kittie".to_string(),
                    }],
                },
            ]
        );
    }

    #[test]
    fn parse_response_accepts_mixed_property_value_types() {
        let response: ParseResponse = serde_json::from_str(
            r#"{
                "parse": {
                    "title": "Porta Batumi Tower",
                    "text": "<p>Body</p>",
                    "properties": {
                        "kartographer_frames": 1,
                        "wikibase_item": "Q125293372"
                    }
                }
            }"#,
        )
        .unwrap();

        let properties = response.parse.unwrap().properties.unwrap();
        assert_eq!(properties.get("kartographer_frames"), Some(&json!(1)));
        assert_eq!(properties.get("wikibase_item"), Some(&json!("Q125293372")));
    }

    #[test]
    fn inline_query_results_are_article_cards() {
        let results = inline_query_results(
            "en",
            &[ArticleButton {
                page_id: 42,
                title: "Batumi railway station".to_string(),
                size: Some(1200),
                wordcount: Some(180),
                snippet: Some("Batumi railway station serves Batumi in Adjara.".to_string()),
                is_disambiguation: false,
            }],
        );

        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["type"], "article");
        assert_eq!(results[0]["id"], "en:42");
        assert_eq!(results[0]["title"], "Batumi railway station");
        assert!(
            results[0]["input_message_content"]["message_text"]
                .as_str()
                .unwrap()
                .contains("<b>Batumi railway station</b>")
        );
        assert!(
            results[0]["input_message_content"]["message_text"]
                .as_str()
                .unwrap()
                .contains("Batumi railway station serves Batumi in Adjara.")
        );
        assert!(
            results[0]["input_message_content"]["message_text"]
                .as_str()
                .unwrap()
                .contains("<a href=\"https://en.wikipedia.org/wiki/Batumi_railway_station\">https://en.wikipedia.org/wiki/Batumi_railway_station</a>")
        );
        assert_eq!(
            results[0]["reply_markup"]["inline_keyboard"][0][0]["url"],
            "https://en.wikipedia.org/wiki/Batumi_railway_station"
        );
    }

    #[test]
    fn inline_extract_preserves_paragraphs_and_message_limit() {
        let extract = normalize_inline_extract("First paragraph.\n\nSecond paragraph.");
        assert_eq!(extract, "First paragraph.\n\nSecond paragraph.");

        let result = ArticleButton {
            page_id: 42,
            title: "Long Article".to_string(),
            size: Some(10_000),
            wordcount: None,
            snippet: Some("Long sentence. ".repeat(500)),
            is_disambiguation: false,
        };
        let message = inline_message_text("en", &result, "https://en.wikipedia.org/wiki/Long");
        assert!(message.chars().count() <= INLINE_MESSAGE_CHAR_LIMIT);
        assert!(message.contains("<b>Long Article</b>"));
        assert!(message.contains("https://en.wikipedia.org/wiki/Long"));
    }

    #[test]
    fn category_names_render_as_clickable_links() {
        let rendered = render_categories_html(
            "en",
            &[
                Category {
                    title: "Category:Games".to_string(),
                    article_count: None,
                },
                Category {
                    title: "Category:Video games".to_string(),
                    article_count: None,
                },
            ],
        );

        assert!(rendered.contains("<b>Categories</b>"));
        assert!(
            rendered
                .contains("<a href=\"https://en.wikipedia.org/wiki/Category%3AGames\">Games</a>")
        );
        assert!(rendered.contains(
            "Games</a>\n<a href=\"https://en.wikipedia.org/wiki/Category%3AVideo_games\">Video games</a>"
        ));
    }

    #[test]
    fn parent_categories_render_with_one_link_per_line() {
        let rendered = render_parent_categories_html(
            "en",
            "Category:Linux games",
            &[
                Category {
                    title: "Category:PC games".to_string(),
                    article_count: None,
                },
                Category {
                    title: "Category:Linux software".to_string(),
                    article_count: None,
                },
            ],
        );

        assert!(rendered.contains("<b>Parent categories of Category:Linux games</b>"));
        assert!(rendered.contains(
            "PC games</a>\n<a href=\"https://en.wikipedia.org/wiki/Category%3ALinux_software\">Linux software</a>"
        ));
    }

    #[test]
    fn html_line_splitter_does_not_break_links() {
        let html = [
            "Metadata",
            "<a href=\"https://example.com/one\">one</a>",
            "<a href=\"https://example.com/two\">two</a>",
        ]
        .join("\n");

        let parts = split_html_lines_for_telegram(&html, 55);

        assert!(parts.len() > 1);
        for part in parts {
            assert_eq!(part.matches("<a ").count(), part.matches("</a>").count());
        }
    }

    #[test]
    fn html_line_splitter_keeps_pre_block_together_when_it_fits() {
        let html = "Intro\n<pre>Year | Total\n1886 | 14,803</pre>\nAfter";

        let parts = split_html_lines_for_telegram(html, 44);

        assert!(
            parts
                .iter()
                .any(|part| part.contains("<pre>Year | Total\n1886 | 14,803</pre>"))
        );
        for part in parts {
            assert_eq!(
                part.matches(TELEGRAM_PRE_OPEN).count(),
                part.matches(TELEGRAM_PRE_CLOSE).count()
            );
        }
    }

    #[test]
    fn html_line_splitter_splits_large_pre_blocks_into_valid_chunks() {
        let html = "<pre>row one\nrow two\nrow three</pre>";

        let parts = split_html_lines_for_telegram(html, 23);

        assert_eq!(
            parts,
            vec![
                "<pre>row one</pre>".to_string(),
                "<pre>row two</pre>".to_string(),
                "<pre>row three</pre>".to_string(),
            ]
        );
        for part in parts {
            assert_eq!(
                part.matches(TELEGRAM_PRE_OPEN).count(),
                part.matches(TELEGRAM_PRE_CLOSE).count()
            );
        }
    }

    #[test]
    fn mediawiki_nav_templates_extract_title_and_article_buttons() {
        let html = r#"
            <div class="navbox">
              <div class="navbox-title"><div class="navbar"><a href="/wiki/Template:Example">v</a><a href="/wiki/Template_talk:Example">t</a><a href="/wiki/Special:EditPage/Template:Example">e</a></div><div>Skyscrapers in Georgia</div></div>
              <a href="/wiki/Template:Example" title="Template:Example">v</a>
              <a href="/wiki/List_of_tallest_buildings_in_Georgia_(country)" title="List of tallest buildings in Georgia (country)">Skyscrapers in Georgia</a>
              <a href="/wiki/Batumi_Tower" title="Batumi Tower">Batumi Tower</a>
              <a class="mw-selflink selflink">Porta Batumi Tower</a>
              <a href="/wiki/Category:Skyscrapers_in_Georgia_(country)" title="Category:Skyscrapers in Georgia (country)">Category</a>
            </div>
        "#;

        let templates = mediawiki_nav_templates("en", html);

        assert_eq!(
            templates,
            vec![NavTemplate {
                title: "Skyscrapers in Georgia".to_string(),
                title_url: None,
                links: vec![
                    NavLink {
                        label: "Skyscrapers in Georgia".to_string(),
                        page_title: "List of tallest buildings in Georgia (country)".to_string(),
                    },
                    NavLink {
                        label: "Batumi Tower".to_string(),
                        page_title: "Batumi Tower".to_string(),
                    },
                ],
            }]
        );
    }

    #[test]
    fn navigation_heading_prefixes_and_links_title_when_available() {
        let html = r#"
            <div class="navbox">
              <div class="navbox-title">
                <div class="navbar"><a href="/wiki/Template:Example">v</a><a href="/wiki/Template_talk:Example">t</a><a href="/wiki/Special:EditPage/Template:Example">e</a></div>
                <a href="/wiki/Flags_of_the_Autonomous_Soviet_Socialist_Republics">Flags of the Autonomous Soviet Socialist Republics</a>
              </div>
              <a href="/wiki/Flag_of_Adjara">Flag of Adjara</a>
            </div>
        "#;

        let templates = mediawiki_nav_templates("en", html);
        assert_eq!(templates.len(), 1);
        assert_eq!(
            templates[0].title,
            "Flags of the Autonomous Soviet Socialist Republics"
        );
        assert_eq!(
            templates[0].title_url.as_deref(),
            Some(
                "https://en.wikipedia.org/wiki/Flags_of_the_Autonomous_Soviet_Socialist_Republics"
            )
        );

        let heading = render_navigation_heading_html(&NavTemplateButtons {
            title: templates[0].title.clone(),
            title_url: templates[0].title_url.clone(),
            buttons: Vec::new(),
        });
        assert_eq!(
            heading,
            "<b>Navigation: <a href=\"https://en.wikipedia.org/wiki/Flags_of_the_Autonomous_Soviet_Socialist_Republics\">Flags of the Autonomous Soviet Socialist Republics</a></b>"
        );
    }

    #[test]
    fn mediawiki_references_render_as_separate_message() {
        let html = r##"
            <ol class="references">
              <li><span class="mw-cite-backlink"><a href="#cite_ref-1">^</a></span>
                <span class="reference-text">See <a href="/wiki/Rust_(programming_language)">Rust</a>.</span>
              </li>
              <li><span class="reference-text">External source <a href="https://example.com/report">report</a>.</span></li>
            </ol>
        "##;

        let rendered = mediawiki_references_to_telegram_html("en", html).unwrap();

        assert!(rendered.contains("1. See"));
        assert!(rendered.contains("Rust."));
        assert!(rendered.contains(
            "<a href=\"https://en.wikipedia.org/wiki/Rust_(programming_language)\">https://en.wikipedia.org/wiki/Rust_(programming_language)</a>"
        ));
        assert!(rendered.contains("\n\n2. External source report."));
        assert!(
            rendered
                .contains("<a href=\"https://example.com/report\">https://example.com/report</a>")
        );
        assert!(!rendered.contains("^"));
    }

    #[test]
    fn metadata_render_includes_last_edit_comments() {
        let metadata = ArticleMetadata {
            language: "en".to_string(),
            commons_url: Some(
                "https://commons.wikimedia.org/wiki/Category:Example_category".to_string(),
            ),
            wikidata_property_count: Some(12),
            info: ArticleInfo {
                page_id: Some(1),
                title: "Example".to_string(),
                full_url: Some("https://en.wikipedia.org/wiki/Example".to_string()),
                length: Some(1234),
                touched: Some("2026-06-05T00:00:00Z".to_string()),
                last_revision_id: Some(99),
                page_props: HashMap::from([
                    ("wikibase_item".to_string(), "Q42".to_string()),
                    ("commonscat".to_string(), "Example category".to_string()),
                ]),
                coordinates: vec![Coordinate {
                    lat: 41.7,
                    lon: 44.8,
                    globe: Some("earth".to_string()),
                }],
            },
            revisions: vec![Revision {
                revid: Some(99),
                timestamp: Some("2026-06-05T00:00:00Z".to_string()),
                user: Some("Editor".to_string()),
                comment: Some("copy edit".to_string()),
                size: Some(1234),
                tags: None,
            }],
            external_links: Vec::new(),
            language_links: Vec::new(),
            pageviews: PageViews {
                days: 30,
                total: 12345,
                latest: Some(("2026-06-04".to_string(), 321)),
            },
        };

        let rendered = render_metadata_html(&metadata);
        assert!(rendered.contains("https://www.wikidata.org/wiki/Q42"));
        assert!(rendered.contains("https://commons.wikimedia.org/wiki/Category:Example_category"));
        assert!(rendered.contains("https://en.wikipedia.org/w/index.php?oldid=99"));
        assert!(
            rendered.contains("https://geohack.toolforge.org/geohack.php?params=41.7_N_44.8_E")
        );
        assert!(rendered.contains("https://en.wikipedia.org/wiki/User:Editor"));
        assert!(rendered.contains("https://pageviews.wmcloud.org/pageviews/"));
        assert!(rendered.contains("Views last 30 days</a>: 12345"));
        assert!(rendered.contains("Latest daily views: 321 on 2026-06-04"));
        assert!(rendered.contains("Editor"));
        assert!(rendered.contains("copy edit"));

        let keyboard = wikidata_metadata_keyboard(&metadata).unwrap();
        let button = &keyboard["inline_keyboard"][0][0];
        assert_eq!(button["callback_data"], "wikidata:en:Q42");
        assert_eq!(button["style"], "danger");
    }

    #[test]
    fn wikidata_commons_url_falls_back_to_p373_category() {
        let entity = WikidataEntity {
            sitelinks: None,
            labels: None,
            descriptions: None,
            claims: Some(HashMap::from([(
                "P373".to_string(),
                vec![WikidataClaim {
                    mainsnak: WikidataSnak {
                        datatype: Some("commonsMedia".to_string()),
                        snaktype: Some("value".to_string()),
                        datavalue: Some(WikidataDataValue {
                            value: Value::String("Vostok 1".to_string()),
                        }),
                    },
                }],
            )])),
        };

        assert_eq!(
            wikidata_commons_url_from_entity(&entity),
            Some("https://commons.wikimedia.org/wiki/Category:Vostok_1".to_string())
        );
    }

    #[test]
    fn wikidata_claims_render_clickable_key_values() {
        let claims = WikidataClaims {
            item: "Q686963".to_string(),
            label: Some("Armies of Exigo".to_string()),
            description: Some("real-time strategy video game".to_string()),
            property_count: 2,
            properties: vec![
                WikidataPropertyClaims {
                    property_id: "P31".to_string(),
                    property_label: "instance of".to_string(),
                    total_values: 1,
                    values: vec![html_link(
                        "https://www.wikidata.org/wiki/Q7889",
                        "video game (Q7889)",
                    )],
                },
                WikidataPropertyClaims {
                    property_id: "P577".to_string(),
                    property_label: "publication date".to_string(),
                    total_values: 1,
                    values: vec![html_escape_text("2004-11-30")],
                },
            ],
        };

        let rendered = render_wikidata_claims_html_parts(&claims, 3900).join("\n\n");

        assert!(rendered.contains(
            "<b><a href=\"https://www.wikidata.org/wiki/Q686963\">Armies of Exigo (Q686963)</a></b>"
        ));
        assert!(rendered.contains(
            "<a href=\"https://www.wikidata.org/wiki/Property:P31\">instance of (P31)</a>: <a href=\"https://www.wikidata.org/wiki/Q7889\">video game (Q7889)</a>"
        ));
        assert!(rendered.contains("Properties: 2"));
    }

    #[test]
    fn wikidata_property_entity_urls_use_property_namespace() {
        assert_eq!(
            wikidata_entity_url("P12245"),
            "https://www.wikidata.org/wiki/Property:P12245"
        );
        assert_eq!(
            wikidata_entity_url("Q686963"),
            "https://www.wikidata.org/wiki/Q686963"
        );
    }

    #[test]
    fn wikidata_url_values_render_clickable() {
        let rendered = render_wikidata_snak_value(
            "P856",
            &WikidataSnak {
                datatype: Some("url".to_string()),
                snaktype: Some("value".to_string()),
                datavalue: Some(WikidataDataValue {
                    value: Value::String("https://example.com/source".to_string()),
                }),
            },
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "<a href=\"https://example.com/source\">https://example.com/source</a>"
        );
    }

    #[test]
    fn wikidata_external_id_values_render_clickable_with_formatter() {
        let rendered = render_wikidata_snak_value(
            "P12245",
            &WikidataSnak {
                datatype: Some("external-id".to_string()),
                snaktype: Some("value".to_string()),
                datavalue: Some(WikidataDataValue {
                    value: Value::String("2888".to_string()),
                }),
            },
            &HashMap::new(),
            &HashMap::from([(
                "P12245".to_string(),
                "https://www.gamesmeter.nl/game/$1".to_string(),
            )]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "<a href=\"https://www.gamesmeter.nl/game/2888\">2888</a>"
        );
    }

    #[test]
    fn wikidata_external_id_formatter_urls_are_url_encoded() {
        assert_eq!(
            wikidata_external_id_url("https://example.com/search?id=$1", "a b/1"),
            Some("https://example.com/search?id=a%20b%2F1".to_string())
        );
    }

    #[test]
    fn wikidata_property_formatter_is_extracted_from_p1630() {
        let entity = WikidataEntity {
            sitelinks: None,
            labels: None,
            descriptions: None,
            claims: Some(HashMap::from([(
                "P1630".to_string(),
                vec![WikidataClaim {
                    mainsnak: WikidataSnak {
                        datatype: Some("string".to_string()),
                        snaktype: Some("value".to_string()),
                        datavalue: Some(WikidataDataValue {
                            value: Value::String("https://www.gamesmeter.nl/game/$1".to_string()),
                        }),
                    },
                }],
            )])),
        };

        assert_eq!(
            wikidata_formatter_url_from_entity(&entity),
            Some("https://www.gamesmeter.nl/game/$1".to_string())
        );
    }

    #[test]
    fn wikidata_page_metadata_renders_last_edits() {
        let metadata = WikidataPageMetadata {
            item: "Q686963".to_string(),
            page_id: Some(123),
            title: "Q686963".to_string(),
            full_url: Some("https://www.wikidata.org/wiki/Q686963".to_string()),
            length: Some(456),
            touched: Some("2026-06-05T00:00:00Z".to_string()),
            last_revision_id: Some(99),
            revisions: vec![Revision {
                revid: Some(99),
                timestamp: Some("2026-06-05T00:00:00Z".to_string()),
                user: Some("Editor".to_string()),
                comment: Some("claim update".to_string()),
                size: Some(456),
                tags: None,
            }],
        };

        let rendered = render_wikidata_page_metadata_html(&metadata);

        assert!(rendered.contains("Wikidata metadata for Q686963"));
        assert!(
            rendered.contains("https://www.wikidata.org/w/index.php?title=Q686963&amp;action=info")
        );
        assert!(rendered.contains("https://www.wikidata.org/w/index.php?oldid=99"));
        assert!(rendered.contains("https://www.wikidata.org/wiki/User:Editor"));
        assert!(rendered.contains("claim update"));
    }
}
