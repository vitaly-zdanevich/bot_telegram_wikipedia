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
const CATEGORY_MEMBER_SCAN_LIMIT: usize = 500;
const CATEGORY_SUBCATEGORY_SCAN_PAGE_LIMIT: usize = 8;
const NAV_TEMPLATE_LINK_LIMIT: usize = 30;
const DISAMBIGUATION_LINK_LIMIT: usize = 30;
const ARTICLE_TITLE_LOOKUP_LIMIT: usize = 50;
const MEDIA_FILE_LOOKUP_LIMIT: usize = 30;
const INLINE_MESSAGE_CHAR_LIMIT: usize = 3900;
const INLINE_CACHE_TIME_SECONDS: u32 = 0;
const REPOSITORY_URL: &str = "https://github.com/vitaly-zdanevich/bot_telegram_wikipedia";

static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();
static RAM_CACHE: OnceLock<Mutex<RamCache>> = OnceLock::new();
static INFOBOX_TABLE_RE: OnceLock<Regex> = OnceLock::new();
static STYLE_SCRIPT_RE: OnceLock<Regex> = OnceLock::new();
static SUP_RE: OnceLock<Regex> = OnceLock::new();
static TAG_RE: OnceLock<Regex> = OnceLock::new();
static COORDINATE_DMS_RE: OnceLock<Regex> = OnceLock::new();

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
        error!(error = %err, "failed to handle Telegram update");
    }

    Ok(response(StatusCode::OK, "ok"))
}

fn response(status: StatusCode, body: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Body::Text(body.to_string()))
        .expect("static response is valid")
}

fn parse_update(body: &Body) -> Result<Update> {
    match body {
        Body::Text(text) => serde_json::from_str(text).context("failed to parse text body"),
        Body::Binary(bytes) => serde_json::from_slice(bytes).context("failed to parse binary body"),
        Body::Empty => bail!("empty body"),
        _ => bail!("unsupported request body type"),
    }
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
}

impl Config {
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
            Err(err) => {
                warn!(error = %err, data, "ignored invalid callback data");
                self.answer_callback_query(&callback.id, "This button is expired.", true)
                    .await?;
            }
        }

        Ok(())
    }

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

    fn is_allowed_user(&self, user_id: Option<i64>) -> bool {
        self.config.allowed_telegram_user_ids.is_empty()
            || user_id.is_some_and(|id| self.config.allowed_telegram_user_ids.contains(&id))
    }

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

    async fn send_category_members(&self, chat_id: i64, language: &str, title: &str) -> Result<()> {
        self.send_chat_action(chat_id, "typing").await?;

        let articles = async {
            self.wiki
                .category_articles(language, title, self.config.search_limit)
                .await
                .with_context(|| {
                    format!(
                        "failed to load category articles for {title} from {language}.wikipedia.org"
                    )
                })
        };
        let subcategories = async {
            self.wiki
                .category_subcategories(language, title, self.config.search_limit)
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
            articles = articles.len(),
            subcategories = subcategories.len(),
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

        if articles.is_empty() && subcategories.is_empty() {
            self.send_message(
                chat_id,
                &format!("No category members found in {title} on {language}.wikipedia.org."),
                favorite_categories_keyboard(&self.config.favorite_categories),
            )
            .await?;
            return Ok(());
        }

        if !subcategories.is_empty() {
            self.send_html_message(
                chat_id,
                &render_category_subcategories_heading_html(language, title),
                category_subcategories_keyboard(language, &subcategories),
            )
            .await?;
        }

        if !articles.is_empty() {
            let reply_markup = json!({
                "inline_keyboard": article_results_keyboard(
                    language,
                    &articles,
                    self.config.big_article_char_threshold,
                    self.config.small_article_char_threshold,
                    articles.len() == 1
                )
            });

            self.send_html_message(
                chat_id,
                &render_category_articles_heading_html(language, title),
                Some(reply_markup),
            )
            .await?;
        }

        Ok(())
    }

    async fn send_article(&self, chat_id: i64, language: String, page_id: u64) -> Result<()> {
        self.send_chat_action(chat_id, "typing").await?;

        let article = self
            .wiki
            .article_content(&language, page_id)
            .await
            .with_context(|| {
                format!("failed to fetch article page_id={page_id} from {language}")
            })?;

        let mut categories_task: Option<JoinHandle<Result<Vec<Category>>>> = None;
        let mut navigation_task: Option<JoinHandle<Result<Vec<NavTemplateButtons>>>> = None;
        let mut disambiguation_task: Option<JoinHandle<Result<Vec<ArticleButton>>>> = None;
        let mut metadata_task: Option<JoinHandle<Result<ArticleMetadata>>> = None;
        let mut media_task: Option<JoinHandle<Result<ArticleMedia>>> = None;

        let parts = render_article_html_parts(&language, &article, self.config.message_char_limit);
        for (index, part) in parts.iter().enumerate() {
            self.send_html_message(chat_id, part, None).await?;
            if index == 0 {
                categories_task = Some(spawn_wiki_task({
                    let wiki = self.wiki.clone();
                    let language = language.clone();
                    async move { wiki.article_categories(&language, page_id).await }
                }));

                if !article.nav_templates.is_empty() {
                    navigation_task = Some(spawn_wiki_task({
                        let wiki = self.wiki.clone();
                        let language = language.clone();
                        let nav_templates = article.nav_templates.clone();
                        async move {
                            wiki.article_buttons_for_nav_templates(&language, &nav_templates)
                                .await
                        }
                    }));
                }

                if !article.disambiguation_links.is_empty() {
                    disambiguation_task = Some(spawn_wiki_task({
                        let wiki = self.wiki.clone();
                        let language = language.clone();
                        let links = article.disambiguation_links.clone();
                        async move { wiki.article_buttons_for_links(&language, &links).await }
                    }));
                }

                metadata_task = Some(spawn_wiki_task({
                    let wiki = self.wiki.clone();
                    let language = language.clone();
                    let revision_limit = self.config.metadata_revision_limit;
                    async move {
                        wiki.article_metadata(&language, page_id, revision_limit)
                            .await
                    }
                }));

                media_task = Some(spawn_wiki_task({
                    let wiki = self.wiki.clone();
                    let language = language.clone();
                    async move { wiki.article_media(&language, page_id).await }
                }));
            }
        }

        if let Some(references_html) = article
            .references_html
            .as_deref()
            .map(str::trim)
            .filter(|references| !references.is_empty())
        {
            for part in
                render_references_html_parts(references_html, self.config.message_char_limit)
            {
                self.send_html_message(chat_id, &part, None).await?;
            }
        }

        if let Some(image) = &article.infobox_image {
            self.send_media_item(
                chat_id,
                &format!("Infobox image for {}", article.title),
                image,
            )
            .await?;
        }

        if let Some(disambiguation_task) = disambiguation_task {
            match join_task(disambiguation_task).await {
                Ok(links) => {
                    if !links.is_empty() {
                        self.send_html_message(
                            chat_id,
                            &html_bold("Disambiguation"),
                            Some(json!({
                                "inline_keyboard": article_results_keyboard(
                                    &language,
                                    &links,
                                    self.config.big_article_char_threshold,
                                    self.config.small_article_char_threshold,
                                    false
                                )
                            })),
                        )
                        .await?;
                    }
                }
                Err(err) => warn!(error = %err, "failed to fetch disambiguation links"),
            }
        }

        if let Some(navigation_task) = navigation_task {
            match join_task(navigation_task).await {
                Ok(templates) => {
                    for template in templates {
                        if template.buttons.is_empty() {
                            continue;
                        }
                        self.send_html_message(
                            chat_id,
                            &render_navigation_heading_html(&template),
                            Some(json!({
                                "inline_keyboard": article_results_keyboard(
                                    &language,
                                    &template.buttons,
                                    self.config.big_article_char_threshold,
                                    self.config.small_article_char_threshold,
                                    false
                                )
                            })),
                        )
                        .await?;
                    }
                }
                Err(err) => warn!(error = %err, "failed to fetch navigation template links"),
            }
        }

        if let Some(categories_task) = categories_task {
            match join_task(categories_task).await {
                Ok(categories) => {
                    if !categories.is_empty() {
                        let categories_html = render_categories_html(&language, &categories);
                        self.send_html_message(
                            chat_id,
                            &categories_html,
                            article_categories_keyboard(&language, page_id, &categories),
                        )
                        .await?;
                    }
                }
                Err(err) => warn!(error = %err, "failed to fetch article categories"),
            }
        }

        if let Some(metadata_task) = metadata_task {
            match join_task(metadata_task).await {
                Ok(metadata) => {
                    let metadata_html =
                        render_metadata_html(&metadata, self.config.message_char_limit);
                    for part in split_html_lines_for_telegram(
                        &metadata_html,
                        self.config.message_char_limit,
                    ) {
                        self.send_html_message(chat_id, &part, None).await?;
                    }
                }
                Err(err) => {
                    warn!(error = %err, "failed to fetch article metadata");
                    self.send_message(chat_id, "Metadata fetch failed.", None)
                        .await?;
                }
            }
        }

        if let Some(media_task) = media_task {
            match join_task(media_task).await {
                Ok(media) => {
                    self.send_media(
                        chat_id,
                        &article.title,
                        &media,
                        article.infobox_image.as_ref(),
                    )
                    .await?
                }
                Err(err) => warn!(error = %err, "failed to fetch article media"),
            }
        }

        Ok(())
    }

    async fn send_media(
        &self,
        chat_id: i64,
        title: &str,
        media: &ArticleMedia,
        already_sent_image: Option<&MediaItem>,
    ) -> Result<()> {
        if let Some(image) = &media.lead_image
            && already_sent_image.is_none_or(|sent| sent.url != image.url)
        {
            self.send_media_item(chat_id, &format!("Image for {title}"), image)
                .await?;
        }

        if let Some(audio) = &media.audio {
            self.send_chat_action(chat_id, "upload_voice").await?;
            if let Err(err) = self
                .telegram_post(
                    "sendAudio",
                    &json!({
                        "chat_id": chat_id,
                        "audio": audio.url,
                        "title": truncate_for_telegram(&audio.title, 64),
                        "caption": truncate_for_telegram(
                            &format!("Audio from {title}"),
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

    async fn send_media_item(&self, chat_id: i64, caption: &str, image: &MediaItem) -> Result<()> {
        self.send_chat_action(chat_id, "upload_photo").await?;
        if let Err(err) = self
            .telegram_post(
                "sendPhoto",
                &json!({
                    "chat_id": chat_id,
                    "photo": image.url,
                    "caption": truncate_for_telegram(caption, 1024)
                }),
            )
            .await
        {
            warn!(error = %err, "failed to send article image");
        }

        Ok(())
    }

    async fn send_message(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        self.send_message_with_parse_mode(chat_id, text, reply_markup, None)
            .await
    }

    async fn send_html_message(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        self.send_message_with_parse_mode(chat_id, text, reply_markup, Some("HTML"))
            .await
    }

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

#[derive(Clone)]
struct WikiClient {
    http: Client,
    cache_max_entries: usize,
}

impl WikiClient {
    fn new(http: Client, cache_max_entries: usize) -> Self {
        Self {
            http,
            cache_max_entries,
        }
    }

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
            ("list", "search".to_string()),
            ("srsearch", query.to_string()),
            ("srlimit", limit.min(MAX_SEARCH_LIMIT).to_string()),
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

        let results = response
            .query
            .search
            .into_iter()
            .map(|hit| ArticleButton {
                page_id: hit.pageid,
                title: html_to_text(&hit.title),
                size: hit.size,
                wordcount: hit.wordcount,
                snippet: hit
                    .snippet
                    .as_deref()
                    .map(html_to_text)
                    .filter(|snippet| !snippet.is_empty()),
            })
            .collect::<Vec<_>>();
        self.cache_put(cache_key, &results, |cache| &mut cache.search);
        Ok(results)
    }

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
            ("prop", "extracts|info".to_string()),
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
                Some(ArticleButton {
                    page_id: page.pageid?,
                    title: page.title,
                    size: page.length,
                    wordcount: None,
                    snippet: page
                        .extract
                        .map(|extract| normalize_inline_extract(&extract))
                        .filter(|snippet| !snippet.is_empty()),
                })
            })
            .collect::<Vec<_>>();
        self.cache_put(cache_key, &results, |cache| &mut cache.search);
        Ok(results)
    }

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

    async fn category_articles(
        &self,
        language: &str,
        title: &str,
        limit: usize,
    ) -> Result<Vec<ArticleButton>> {
        let normalized_title = normalize_category_title_for_language(title, language);
        let cache_key = format!("category_articles:{language}:{limit}:{normalized_title}");
        if let Some(articles) = self.cache_get(&cache_key, |cache| &mut cache.category_articles) {
            return Ok(articles);
        }

        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("generator", "categorymembers".to_string()),
            ("gcmtitle", normalized_title.clone()),
            ("gcmnamespace", "0".to_string()),
            ("gcmsort", "timestamp".to_string()),
            ("gcmdir", "desc".to_string()),
            ("gcmprop", "ids|title|timestamp".to_string()),
            ("gcmlimit", limit.min(MAX_SEARCH_LIMIT).to_string()),
            ("prop", "info".to_string()),
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

        let articles = response
            .query
            .map(|query| query.pages)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|page| Some((page.pageid?, page.title, page.length)))
            .map(|(page_id, title, length)| ArticleButton {
                page_id,
                title,
                size: length,
                wordcount: None,
                snippet: None,
            })
            .collect::<Vec<_>>();
        self.cache_put(cache_key, &articles, |cache| &mut cache.category_articles);
        Ok(articles)
    }

    async fn category_subcategories(
        &self,
        language: &str,
        title: &str,
        limit: usize,
    ) -> Result<Vec<Category>> {
        let normalized_title = normalize_category_title_for_language(title, language);
        let cache_key = format!("category_subcategories:{language}:{limit}:{normalized_title}");
        if let Some(categories) =
            self.cache_get(&cache_key, |cache| &mut cache.category_subcategories)
        {
            return Ok(categories);
        }

        let limit = limit.min(MAX_SEARCH_LIMIT);
        let mut categories = Vec::new();
        let mut seen = HashSet::new();
        let mut continuation: Option<String> = None;

        for _ in 0..CATEGORY_SUBCATEGORY_SCAN_PAGE_LIMIT {
            let mut params = vec![
                ("action", "query".to_string()),
                ("format", "json".to_string()),
                ("formatversion", "2".to_string()),
                ("list", "categorymembers".to_string()),
                ("cmtitle", normalized_title.clone()),
                ("cmnamespace", "14".to_string()),
                ("cmsort", "timestamp".to_string()),
                ("cmdir", "desc".to_string()),
                ("cmprop", "ids|title|timestamp".to_string()),
                ("cmlimit", CATEGORY_MEMBER_SCAN_LIMIT.to_string()),
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

            for member in response
                .query
                .map(|query| query.categorymembers)
                .unwrap_or_default()
            {
                let title = normalize_category_title_for_language(&member.title, language);
                let key = title.to_lowercase();
                if seen.insert(key) {
                    categories.push(Category {
                        title,
                        article_count: None,
                    });
                }
                if categories.len() >= limit {
                    break;
                }
            }

            if categories.len() >= limit {
                break;
            }

            continuation = response
                .continuation
                .and_then(|continuation| continuation.cmcontinue);
            if continuation.is_none() {
                break;
            }
        }

        self.cache_put(cache_key, &categories, |cache| {
            &mut cache.category_subcategories
        });
        Ok(categories)
    }

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
            ("prop", "extracts|categories".to_string()),
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
            description,
            parent_categories,
        };
        self.cache_put(cache_key, &overview, |cache| &mut cache.category_overviews);
        Ok(overview)
    }

    async fn article_content(&self, language: &str, page_id: u64) -> Result<ArticleContent> {
        let cache_key = format!("article_content:{language}:{page_id}");
        if let Some(article) = self.cache_get(&cache_key, |cache| &mut cache.article_content) {
            return Ok(article);
        }

        let article = self.article_parsed_html(language, page_id).await?;
        self.cache_put(cache_key, &article, |cache| &mut cache.article_content);
        Ok(article)
    }

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

        let response: ParseResponse = self
            .http
            .get(wiki_api_url(language)?)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

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
        let body_html = mediawiki_html_to_telegram_html(language, &title, &html);
        let extract = body_html
            .as_deref()
            .map(html_to_plain_text)
            .unwrap_or_else(|| html_to_text(&html));
        let length = (!extract.is_empty()).then_some(extract.chars().count());
        let is_disambiguation = is_disambiguation_page(&parsed, &html);
        let disambiguation_links = if is_disambiguation {
            mediawiki_disambiguation_links(language, &html)
        } else {
            Vec::new()
        };

        Ok(ArticleContent {
            title,
            extract,
            length,
            infobox: extract_infobox_text(language, &html),
            infobox_image: extract_infobox_image(&html),
            body_html,
            references_html: mediawiki_references_to_telegram_html(language, &html),
            nav_templates: mediawiki_nav_templates(language, &html),
            is_disambiguation,
            disambiguation_links,
        })
    }

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
                })
            })
            .collect())
    }

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
        let commons_url = match commons_page_url(&parts.info.page_props) {
            Some(url) => Some(url),
            None => {
                if let Some(wikidata_item) = parts.info.page_props.get("wikibase_item") {
                    self.wikidata_commons_url(wikidata_item)
                        .await
                        .unwrap_or_else(|err| {
                            warn!(error = %err, wikidata_item, "failed to fetch Commons link from Wikidata");
                            None
                        })
                } else {
                    None
                }
            }
        };
        let metadata = ArticleMetadata {
            language: language.to_string(),
            info: parts.info,
            commons_url,
            revisions: parts.revisions,
            external_links: parts.external_links,
            language_links: parts.language_links,
            pageviews: parts.pageviews,
        };
        self.cache_put(cache_key, &metadata, |cache| &mut cache.article_metadata);
        Ok(metadata)
    }

    async fn wikidata_commons_url(&self, wikidata_item: &str) -> Result<Option<String>> {
        let params = vec![
            ("action", "wbgetentities".to_string()),
            ("format", "json".to_string()),
            ("ids", wikidata_item.to_string()),
            ("props", "sitelinks|claims".to_string()),
            ("sitefilter", "commonswiki".to_string()),
        ];

        let response: WikidataEntitiesResponse = self
            .http
            .get("https://www.wikidata.org/w/api.php")
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(response
            .entities
            .get(wikidata_item)
            .and_then(wikidata_commons_url_from_entity))
    }

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
            ("pithumbsize", "1280".to_string()),
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
                mime: None,
            }),
            audio: None,
        };

        let file_titles = page
            .images
            .unwrap_or_default()
            .into_iter()
            .map(|image| image.title)
            .filter(|title| looks_like_image_or_audio(title))
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

        media.audio = file_infos.into_iter().find(|item| {
            item.mime
                .as_deref()
                .is_some_and(|mime| mime.starts_with("audio/"))
        });

        self.cache_put(cache_key, &media, |cache| &mut cache.article_media);
        Ok(media)
    }

    async fn file_infos(&self, language: &str, titles: &[String]) -> Result<Vec<MediaItem>> {
        let params = vec![
            ("action", "query".to_string()),
            ("format", "json".to_string()),
            ("formatversion", "2".to_string()),
            ("prop", "imageinfo".to_string()),
            ("titles", titles.join("|")),
            ("iiprop", "url|mime|size|mediatype".to_string()),
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
            .filter_map(|page| {
                let info = page.imageinfo?.into_iter().next()?;
                Some(MediaItem {
                    title: page.title,
                    url: info.url,
                    mime: info.mime,
                })
            })
            .collect())
    }

    fn cache_get<T, F>(&self, key: &str, select: F) -> Option<T>
    where
        T: Clone,
        F: for<'a> FnOnce(&'a mut RamCache) -> &'a mut HashMap<String, CacheEntry<T>>,
    {
        ram_cache_get(key, select)
    }

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
    category_articles: HashMap<String, CacheEntry<Vec<ArticleButton>>>,
    category_subcategories: HashMap<String, CacheEntry<Vec<Category>>>,
    category_overviews: HashMap<String, CacheEntry<CategoryOverview>>,
    article_content: HashMap<String, CacheEntry<ArticleContent>>,
    article_categories: HashMap<String, CacheEntry<Vec<Category>>>,
    article_metadata: HashMap<String, CacheEntry<ArticleMetadata>>,
    article_media: HashMap<String, CacheEntry<ArticleMedia>>,
}

#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
}

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

fn spawn_wiki_task<T>(
    future: impl std::future::Future<Output = Result<T>> + Send + 'static,
) -> JoinHandle<Result<T>>
where
    T: Send + 'static,
{
    tokio::spawn(future)
}

async fn join_task<T>(handle: JoinHandle<Result<T>>) -> Result<T> {
    handle.await.context("background task panicked")?
}

fn wiki_api_url(language: &str) -> Result<String> {
    let language = normalize_language_code(language).context("invalid Wikipedia language code")?;
    Ok(format!("https://{language}.wikipedia.org/w/api.php"))
}

fn required_env(key: &str) -> Result<String> {
    env::var(key).with_context(|| format!("{key} is required"))
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
}

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

fn category_namespace_prefix(language: &str) -> &'static str {
    match language {
        "be" => "Катэгорыя",
        "ru" => "Категория",
        _ => "Category",
    }
}

fn category_display_title(title: &str) -> String {
    title
        .strip_prefix("Category:")
        .or_else(|| title.strip_prefix("Категория:"))
        .or_else(|| title.strip_prefix("Катэгорыя:"))
        .unwrap_or(title)
        .trim()
        .to_string()
}

fn category_search_query(query: &str) -> String {
    let query = query.trim();
    if query.split_whitespace().count() == 1 {
        format!("intitle:{query}")
    } else {
        query.to_string()
    }
}

fn category_title_matches_query(title: &str, query: &str) -> bool {
    let title = category_display_title(title).to_lowercase();
    let query = query.to_lowercase();
    query
        .split_whitespace()
        .filter(|part| !part.is_empty())
        .all(|part| title.contains(part))
}

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

fn parse_category_value(value: &str, default_language: &str) -> Result<CategoryRequest> {
    parse_category_value_with_forced_language(value, default_language, None)
}

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
    ArticleCategory {
        language: String,
        page_id: u64,
        index: usize,
    },
}

fn parse_callback_data(
    data: &str,
    favorite_categories: &[FavoriteCategory],
) -> Result<CallbackAction> {
    if let Some(rest) = data.strip_prefix("article:") {
        let mut parts = rest.split(':');
        let language = parts
            .next()
            .and_then(normalize_language_code)
            .context("article callback has invalid language")?;
        let page_id = parts
            .next()
            .context("article callback has no page id")?
            .parse::<u64>()
            .context("article callback page id is invalid")?;
        if parts.next().is_some() {
            bail!("article callback has extra fields");
        }
        return Ok(CallbackAction::Article { language, page_id });
    }

    if let Some(rest) = data.strip_prefix("article_category:") {
        let mut parts = rest.split(':');
        let language = parts
            .next()
            .and_then(normalize_language_code)
            .context("article category callback has invalid language")?;
        let page_id = parts
            .next()
            .context("article category callback has no page id")?
            .parse::<u64>()
            .context("article category callback page id is invalid")?;
        let index = parts
            .next()
            .context("article category callback has no index")?
            .parse::<usize>()
            .context("article category callback index is invalid")?;
        if parts.next().is_some() {
            bail!("article category callback has extra fields");
        }
        return Ok(CallbackAction::ArticleCategory {
            language,
            page_id,
            index,
        });
    }

    if let Some(rest) = data.strip_prefix("category:") {
        let mut parts = rest.split(':');
        let language = parts
            .next()
            .and_then(normalize_language_code)
            .context("category callback has invalid language")?;
        let encoded = parts.next().context("category callback has no title")?;
        if parts.next().is_some() {
            bail!("category callback has extra fields");
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .context("category callback title is not base64url")?;
        let title = String::from_utf8(bytes).context("category callback title is not utf-8")?;
        return Ok(CallbackAction::Category {
            title: normalize_category_title_for_language(&title, &language),
            language,
        });
    }

    if let Some(rest) = data.strip_prefix("favorite_category:") {
        let index = rest
            .parse::<usize>()
            .context("favorite category callback index is invalid")?;
        let favorite = favorite_categories
            .get(index)
            .context("favorite category index is out of range")?;
        return Ok(CallbackAction::Category {
            language: favorite.language.clone(),
            title: favorite.title.clone(),
        });
    }

    bail!("unknown callback data")
}

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

fn normalize_inline_extract(value: &str) -> String {
    value
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

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

fn category_search_keyboard(language: &str, categories: &[Category]) -> Option<Value> {
    let rows = category_buttons_rows(language, categories, None);
    if rows.is_empty() {
        return None;
    }

    Some(json!({ "inline_keyboard": rows }))
}

fn category_subcategories_keyboard(language: &str, subcategories: &[Category]) -> Option<Value> {
    let rows = category_buttons_rows(
        language,
        subcategories,
        subcategory_button_style(subcategories.len()),
    );
    if rows.is_empty() {
        return None;
    }

    Some(json!({ "inline_keyboard": rows }))
}

fn subcategory_button_style(count: usize) -> Option<&'static str> {
    if count > 10 {
        Some("danger")
    } else if count == 1 {
        Some("primary")
    } else {
        None
    }
}

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

fn article_category_button_style(article_count: Option<usize>) -> Option<&'static str> {
    match article_count {
        Some(1) => Some("primary"),
        Some(count) if count > 10 => Some("danger"),
        _ => None,
    }
}

fn category_title_callback_data(language: &str, title: &str) -> Option<String> {
    let encoded = URL_SAFE_NO_PAD.encode(normalize_category_title_for_language(title, language));
    let data = format!("category:{language}:{encoded}");
    (data.len() <= 64).then_some(data)
}

fn article_category_callback_data(language: &str, page_id: u64, index: usize) -> String {
    format!("article_category:{language}:{page_id}:{index}")
}

fn article_url(language: &str, title: &str) -> String {
    format!(
        "https://{language}.wikipedia.org/wiki/{}",
        urlencoding::encode(&title.replace(' ', "_"))
    )
}

fn render_article_html_parts(
    language: &str,
    article: &ArticleContent,
    limit: usize,
) -> Vec<String> {
    let mut sections = Vec::new();
    sections.push(html_bold_link(
        &article_url(language, &article.title),
        &article.title,
    ));

    if article.is_disambiguation {
        sections.push(format!(
            "{}\n{}",
            html_bold("Disambiguation page"),
            html_escape_text("Choose a target article from the buttons below.")
        ));
        return combine_html_sections(sections, limit);
    }

    if let Some(infobox) = &article.infobox {
        sections.push(format!(
            "{}\n{}",
            html_bold("Infobox"),
            render_infobox_html(infobox, article_infobox_limit(article.length))
        ));
    }

    let body_parts = if let Some(body_html) = article
        .body_html
        .as_deref()
        .map(str::trim)
        .filter(|body| !body.is_empty())
    {
        split_telegram_text(body_html, limit.saturating_sub(512).max(1))
    } else {
        let extract = article.extract.trim();
        let extract = if extract.is_empty() {
            "Wikipedia did not return a plain-text extract for this page."
        } else {
            extract
        };
        split_telegram_text(extract, limit.saturating_sub(512).max(1))
            .into_iter()
            .map(|part| html_escape_text(&part))
            .collect::<Vec<_>>()
    };
    for part in body_parts {
        sections.push(part);
    }

    combine_html_sections(sections, limit)
}

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

fn render_category_subcategories_heading_html(language: &str, title: &str) -> String {
    render_category_heading_html(language, title, "Subcategories in")
}

fn render_category_articles_heading_html(language: &str, title: &str) -> String {
    render_category_heading_html(language, title, "Newest article pages in")
}

fn render_category_heading_html(language: &str, title: &str, prefix: &str) -> String {
    let title = normalize_category_title_for_language(title, language);
    let linked_title = html_link(&article_url(language, &title), &title);
    format!("{prefix} {linked_title}")
}

fn render_references_html_parts(references_html: &str, limit: usize) -> Vec<String> {
    combine_html_sections(
        vec![format!("{}\n{}", html_bold("References"), references_html)],
        limit,
    )
}

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
            parts.extend(split_telegram_text(&section, limit));
        } else {
            current = section;
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

fn html_bold(value: &str) -> String {
    format!("<b>{}</b>", html_escape_text(value))
}

fn html_link(url: &str, text: &str) -> String {
    format!(
        "<a href=\"{}\">{}</a>",
        html_escape::encode_double_quoted_attribute(url),
        html_escape_text(text)
    )
}

fn html_bold_link(url: &str, text: &str) -> String {
    format!(
        "<b><a href=\"{}\">{}</a></b>",
        html_escape::encode_double_quoted_attribute(url),
        html_escape_text(text)
    )
}

fn html_escape_text(value: &str) -> String {
    html_escape::encode_text(value).to_string()
}

fn mediawiki_html_to_telegram_html(language: &str, title: &str, html: &str) -> Option<String> {
    let document = Html::parse_fragment(html);
    let root = document.root_element();
    let mut sections = Vec::new();
    collect_html_blocks(language, title, root, &mut sections);
    (!sections.is_empty()).then(|| sections.join("\n\n"))
}

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

fn is_disambiguation_page(parsed: &ParsedPage, html: &str) -> bool {
    parsed
        .properties
        .as_ref()
        .is_some_and(|properties| properties.contains_key("disambiguation"))
        || html.contains("id=\"disambigbox\"")
        || html.contains("dmbox-disambig")
}

fn mediawiki_disambiguation_links(language: &str, html: &str) -> Vec<NavLink> {
    let document = Html::parse_fragment(html);
    let list_item_selector = Selector::parse("li").expect("list item selector is valid");
    let link_selector = Selector::parse("a[href]").expect("link selector is valid");
    let mut seen = HashSet::new();
    let mut links = Vec::new();

    for item in document.select(&list_item_selector) {
        if element_or_ancestor_has_class_part(
            item,
            &[
                "metadata",
                "navbox",
                "sistersitebox",
                "selfreference",
                "reflist",
                "reference",
                "portal",
                "noprint",
            ],
        ) {
            continue;
        }

        let Some(link) = item.select(&link_selector).find(|link| {
            !element_has_class_part(*link, &["selflink", "new", "mw-disambig"])
                && link
                    .value()
                    .attr("href")
                    .and_then(wiki_article_title_from_href)
                    .is_some()
        }) else {
            continue;
        };

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
        links.push(NavLink {
            label: if label.is_empty() {
                page_title.clone()
            } else {
                label.to_string()
            },
            page_title,
        });

        if links.len() >= DISAMBIGUATION_LINK_LIMIT {
            break;
        }
    }

    links
}

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

fn normalized_title_key(title: &str) -> String {
    title
        .replace('_', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

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

fn push_unique_link(links: &mut Vec<String>, url: String) {
    if !links.iter().any(|link| link == &url) {
        links.push(url);
    }
}

fn collect_html_blocks(
    language: &str,
    title: &str,
    element: ElementRef<'_>,
    sections: &mut Vec<String>,
) {
    if should_skip_block(element) {
        return;
    }

    let name = element.value().name();
    match name {
        "p" => {
            let rendered = trim_html_spacing(&render_inline_element(language, element));
            if !rendered.is_empty() {
                sections.push(rendered);
            }
        }
        "blockquote" => {
            let rendered = trim_html_spacing(&render_inline_element(language, element));
            if !rendered.is_empty() {
                sections.push(format!("<blockquote>{rendered}</blockquote>"));
            }
        }
        "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
            let rendered = trim_html_spacing(&render_inline_element(language, element));
            let heading_text = html_to_plain_text(&rendered);
            if !rendered.is_empty() && !is_terminal_article_heading(&heading_text) {
                let header = element_anchor_id(element)
                    .map(|anchor| {
                        html_bold_link(
                            &format!("{}#{anchor}", article_url(language, title)),
                            &heading_text,
                        )
                    })
                    .unwrap_or_else(|| format!("<b>{rendered}</b>"));
                sections.push(header);
            }
        }
        "ul" => {
            let rendered = render_list(language, element, false);
            if !rendered.is_empty() {
                sections.push(rendered);
            }
        }
        "ol" => {
            let rendered = render_list(language, element, true);
            if !rendered.is_empty() {
                sections.push(rendered);
            }
        }
        "table" => {
            let rendered = render_table(language, element);
            if !rendered.is_empty() {
                sections.push(rendered);
            }
        }
        _ => {
            for child in element.children() {
                if let Some(child_element) = ElementRef::wrap(child) {
                    collect_html_blocks(language, title, child_element, sections);
                }
            }
        }
    }
}

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

fn render_table(language: &str, table: ElementRef<'_>) -> String {
    let mut rows = Vec::new();
    collect_table_rows(table, &mut rows);

    rows.into_iter()
        .filter_map(|row| render_table_row(language, row))
        .collect::<Vec<_>>()
        .join("\n")
}

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

fn render_table_row(language: &str, row: ElementRef<'_>) -> Option<String> {
    let cells = row
        .children()
        .filter_map(ElementRef::wrap)
        .filter(|cell| matches!(cell.value().name(), "th" | "td"))
        .filter_map(|cell| {
            let rendered = trim_html_spacing(&render_inline_element(language, cell));
            let rendered = rendered
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .collect::<Vec<_>>()
                .join(" / ");
            (!rendered.is_empty()).then_some(rendered)
        })
        .collect::<Vec<_>>();

    (!cells.is_empty()).then(|| cells.join(" | "))
}

fn render_inline_element(language: &str, element: ElementRef<'_>) -> String {
    let mut out = String::new();
    render_inline_children(language, element, &mut out);
    trim_html_spacing(&out)
}

fn render_inline_children(language: &str, element: ElementRef<'_>, out: &mut String) {
    for child in element.children() {
        render_inline_node(language, child, out);
    }
}

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

fn render_inline_wrapped(language: &str, element: ElementRef<'_>, out: &mut String, tag: &str) {
    let inner = render_inline_element(language, element);
    if !inner.is_empty() {
        out.push_str(&format!("<{tag}>{inner}</{tag}>"));
    }
}

fn should_skip_block(element: ElementRef<'_>) -> bool {
    let name = element.value().name();
    matches!(
        name,
        "style" | "script" | "noscript" | "figure" | "img" | "audio" | "video"
    ) || element_has_class_part(
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

fn element_has_class_part(element: ElementRef<'_>, needles: &[&str]) -> bool {
    element.value().attr("class").is_some_and(|classes| {
        classes.split_whitespace().any(|class| {
            let class = class.to_ascii_lowercase();
            needles.iter().any(|needle| class.contains(needle))
        })
    })
}

fn element_or_ancestor_has_class_part(element: ElementRef<'_>, needles: &[&str]) -> bool {
    element_has_class_part(element, needles)
        || element
            .ancestors()
            .filter_map(ElementRef::wrap)
            .any(|ancestor| element_has_class_part(ancestor, needles))
}

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

fn append_single_space(out: &mut String) {
    if !out.is_empty() && !out.ends_with(' ') && !out.ends_with('\n') {
        out.push(' ');
    }
}

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

fn html_to_plain_text(value: &str) -> String {
    html_to_text(value)
}

fn is_terminal_article_heading(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "references" | "notes" | "external links" | "further reading" | "bibliography" | "sources"
    )
}

fn article_infobox_limit(article_length: Option<usize>) -> usize {
    if article_length.is_some_and(|length| length > 50_000) {
        1400
    } else {
        1800
    }
}

fn render_metadata_html(metadata: &ArticleMetadata, limit: usize) -> String {
    let info = &metadata.info;
    let mut lines = Vec::new();
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
        lines.push(format!(
            "Coordinates: {}{}",
            html_link(
                &coordinate_url(coordinate.lat, coordinate.lon),
                &format!("{}, {}", coordinate.lat, coordinate.lon)
            ),
            coordinate
                .globe
                .as_ref()
                .map(|globe| format!(" ({})", html_escape_text(globe)))
                .unwrap_or_default()
        ));
    }
    if metadata.pageviews.days > 0 {
        lines.push(format!(
            "{}: {}",
            html_link(
                &pageviews_url(&metadata.language, &info.title),
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
    lines.push(format!("Language links: {}", metadata.language_links.len()));
    if !metadata.language_links.is_empty() {
        lines.push(format!(
            "Languages: {}",
            metadata
                .language_links
                .iter()
                .take(12)
                .map(|link| {
                    let label = link
                        .langname
                        .as_deref()
                        .map(|name| format!("{} ({})", link.lang, name))
                        .unwrap_or_else(|| link.lang.clone());
                    html_escape_text(&label)
                })
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    lines.push(format!("External links: {}", metadata.external_links.len()));
    for link in metadata.external_links.iter().take(8) {
        lines.push(format!("External: {}", html_link(link, link)));
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
            let revision = revision.clone();
            let revision_text = revision
                .revid
                .map(|revid| revision_link(&metadata.language, revid))
                .unwrap_or_else(|| "rev 0".to_string());
            lines.push(format!(
                "- {} | {} | {} | size {} | {}{}",
                html_escape_text(revision.timestamp.as_deref().unwrap_or("unknown time")),
                revision_user_link(&metadata.language, revision.user.as_deref()),
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

    truncate_for_telegram(&lines.join("\n"), limit)
}

fn revision_link(language: &str, revision_id: u64) -> String {
    html_link(
        &format!("https://{language}.wikipedia.org/w/index.php?oldid={revision_id}"),
        &format!("rev {revision_id}"),
    )
}

fn pageviews_url(language: &str, title: &str) -> String {
    format!(
        "https://pageviews.wmcloud.org/pageviews/?project={language}.wikipedia.org&platform=all-access&agent=user&redirects=0&range=latest-30&pages={}",
        urlencoding::encode(&title.replace(' ', "_"))
    )
}

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

fn commons_title_url(title: &str) -> String {
    format!(
        "https://commons.wikimedia.org/wiki/{}",
        urlencoding::encode(&title.trim().replace(' ', "_"))
    )
}

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

fn capture_number(captures: &regex::Captures<'_>, name: &str) -> Option<f64> {
    captures.name(name)?.as_str().parse::<f64>().ok()
}

fn coordinate_direction_is_negative(direction: &str) -> bool {
    let direction = direction.trim().to_lowercase();
    direction.starts_with('s')
        || direction.starts_with('w')
        || direction.starts_with('ю')
        || direction.starts_with('з')
}

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

fn split_html_lines_for_telegram(html: &str, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    let mut parts = Vec::new();
    let mut current = String::new();

    for line in html.lines() {
        let line = if line.chars().count() > limit {
            html_escape_text(&truncate_for_telegram(&html_to_plain_text(line), limit))
        } else {
            line.to_string()
        };

        if current.is_empty() {
            current = line;
            continue;
        }

        if current.chars().count() + 1 + line.chars().count() <= limit {
            current.push('\n');
            current.push_str(&line);
        } else {
            parts.push(current);
            current = line;
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

fn split_long_word(word: &str, limit: usize) -> Vec<String> {
    let chars = word.chars().collect::<Vec<_>>();
    chars
        .chunks(limit)
        .map(|chunk| chunk.iter().collect::<String>())
        .collect()
}

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

fn infobox_cell_html(language: &str, cell: ElementRef<'_>) -> String {
    clean_infobox_text(&trim_html_spacing(&render_inline_element(language, cell)))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" / ")
}

fn is_meaningful_unlabeled_infobox_line(line: &str) -> bool {
    parse_coordinate_pair(line).is_some() || line.chars().count() >= 24
}

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

            let title = image
                .ancestors()
                .filter_map(ElementRef::wrap)
                .find(|element| element.value().name() == "a")
                .and_then(|link| link.value().attr("title"))
                .unwrap_or("Infobox image")
                .to_string();

            return Some(MediaItem {
                title,
                url: src,
                mime: None,
            });
        }
    }

    None
}

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

fn is_empty_infobox_label(line: &str) -> bool {
    line.trim()
        .strip_suffix(':')
        .is_some_and(|label| !label.trim().is_empty())
}

fn is_infobox_map_caption(line: &str) -> bool {
    let line = line.trim().to_ascii_lowercase();
    line.starts_with("interactive map")
        || line.starts_with("show map")
        || line.starts_with("show all")
}

fn is_infobox_sister_project_line(line: &str) -> bool {
    let line = line.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = line.to_lowercase();
    (lower.contains("викисклад") && lower.contains("медиафайл"))
        || lower == "wikimedia commons"
        || lower == "media files at wikimedia commons"
        || lower == "media related to wikimedia commons"
}

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
}

#[derive(Debug, Clone)]
struct ArticleContent {
    title: String,
    extract: String,
    length: Option<usize>,
    infobox: Option<String>,
    infobox_image: Option<MediaItem>,
    body_html: Option<String>,
    references_html: Option<String>,
    nav_templates: Vec<NavTemplate>,
    is_disambiguation: bool,
    disambiguation_links: Vec<NavLink>,
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
struct CategoryOverview {
    description: Option<String>,
    parent_categories: Vec<Category>,
}

#[derive(Debug, Clone)]
struct ArticleMetadata {
    language: String,
    info: ArticleInfo,
    commons_url: Option<String>,
    revisions: Vec<Revision>,
    external_links: Vec<String>,
    language_links: Vec<LanguageLink>,
    pageviews: PageViews,
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
    audio: Option<MediaItem>,
}

#[derive(Debug, Clone)]
struct MediaItem {
    title: String,
    url: String,
    mime: Option<String>,
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
    pageid: u64,
    title: String,
    snippet: Option<String>,
    size: Option<usize>,
    wordcount: Option<usize>,
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

#[derive(Debug, Deserialize)]
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
    properties: Option<HashMap<String, String>>,
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
    pages: usize,
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
}

#[derive(Debug, Deserialize)]
struct WikidataEntitiesResponse {
    entities: HashMap<String, WikidataEntity>,
}

#[derive(Debug, Deserialize)]
struct WikidataEntity {
    sitelinks: Option<HashMap<String, WikidataSitelink>>,
    claims: Option<HashMap<String, Vec<WikidataClaim>>>,
}

#[derive(Debug, Deserialize)]
struct WikidataSitelink {
    title: String,
}

#[derive(Debug, Deserialize)]
struct WikidataClaim {
    mainsnak: WikidataSnak,
}

#[derive(Debug, Deserialize)]
struct WikidataSnak {
    datavalue: Option<WikidataDataValue>,
}

#[derive(Debug, Deserialize)]
struct WikidataDataValue {
    value: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

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
            CallbackAction::Article { .. } | CallbackAction::Category { .. } => {
                panic!("expected article category callback")
            }
        }
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
        let rendered = render_category_subcategories_heading_html("en", "Category:PC games");

        assert!(rendered.starts_with("Subcategories in "));
        assert!(rendered.contains(
            "<a href=\"https://en.wikipedia.org/wiki/Category%3APC_games\">Category:PC games</a>"
        ));

        let rendered = render_category_articles_heading_html("en", "Category:PC games");
        assert!(rendered.starts_with("Newest article pages in "));
        assert!(rendered.contains(
            "<a href=\"https://en.wikipedia.org/wiki/Category%3APC_games\">Category:PC games</a>"
        ));
    }

    #[test]
    fn single_subcategory_button_uses_primary_style() {
        let keyboard = category_subcategories_keyboard(
            "en",
            &[Category {
                title: "Category:PC games by genre".to_string(),
                article_count: None,
            }],
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

        let keyboard = category_subcategories_keyboard("en", &subcategories).unwrap();

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
            }],
            DEFAULT_BIG_ARTICLE_CHAR_THRESHOLD,
            DEFAULT_SMALL_ARTICLE_CHAR_THRESHOLD,
            false,
        );

        assert_eq!(keyboard[0][0]["callback_data"], "article:en:43");
        assert_eq!(keyboard[0][0]["style"], "primary");
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
            "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a1/Flag.svg/320px-Flag.svg.png"
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
            references_html: None,
            nav_templates: Vec::new(),
            is_disambiguation: false,
            disambiguation_links: Vec::new(),
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
        assert!(rendered.contains("Name | Use"));
        assert!(rendered.contains(
            "<a href=\"https://en.wikipedia.org/wiki/Batumi_Tower\">Batumi Tower</a> | Skyscraper / hotel"
        ));
        assert!(!rendered.contains("[1]"));
        assert!(!rendered.contains("Reference text"));
    }

    #[test]
    fn disambiguation_pages_render_target_buttons_instead_of_body() {
        let parsed = ParsedPage {
            title: Some("Mercury".to_string()),
            text: None,
            properties: Some(HashMap::from([(
                "disambiguation".to_string(),
                String::new(),
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
            mediawiki_disambiguation_links("en", html),
            vec![
                NavLink {
                    label: "Mercury (planet)".to_string(),
                    page_title: "Mercury (planet)".to_string(),
                },
                NavLink {
                    label: "Mercury (element)".to_string(),
                    page_title: "Mercury (element)".to_string(),
                },
            ]
        );

        let article = ArticleContent {
            title: "Mercury".to_string(),
            extract: "large disambiguation body".to_string(),
            length: Some(9999),
            infobox: None,
            infobox_image: None,
            body_html: Some("This body must not be dumped.".to_string()),
            references_html: None,
            nav_templates: Vec::new(),
            is_disambiguation: true,
            disambiguation_links: mediawiki_disambiguation_links("en", html),
        };
        let rendered = render_article_html_parts("en", &article, 3900).join("\n\n");
        assert!(rendered.contains("Disambiguation page"));
        assert!(rendered.contains("Choose a target article"));
        assert!(!rendered.contains("This body must not be dumped"));
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

        let rendered = render_metadata_html(&metadata, 3900);
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
    }

    #[test]
    fn wikidata_commons_url_falls_back_to_p373_category() {
        let entity = WikidataEntity {
            sitelinks: None,
            claims: Some(HashMap::from([(
                "P373".to_string(),
                vec![WikidataClaim {
                    mainsnak: WikidataSnak {
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
}
