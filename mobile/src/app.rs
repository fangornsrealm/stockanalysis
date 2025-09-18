use dioxus::prelude::*;
use ui::components::sidebar::SideBar;
use ui::components::utils::Loading;
use ui::dashboards::home::Home;
use ui::dashboards::performance::Performance;
use ui::dashboards::options::Options;
use ui::dashboards::portfolio::Portfolio;
use ui::dashboards::screener::Screener;
use ui::dashboards::financials::Financials;
use ui::dashboards::news::News;
use ui::app::Page;

#[component]
pub fn App() -> Element {
    let active_page = use_signal(|| Page::Home);

    rsx! {
        head {
            link {
                href: "https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css",
                rel: "stylesheet"
            }
            link {
                href: "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.7.2/font/bootstrap-icons.css",
                rel: "stylesheet"
            }
            link {
                href: "https://cdn.jsdelivr.net/gh/devicons/devicon@latest/devicon.min.css",
                rel: "stylesheet"
            }
            link {
                href: "https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap",
                rel: "stylesheet"
            }
        }

        div {
            style: "display: flex; height: 100vh; font-family: 'Poppins', sans-serif;",

            SideBar { active_page: active_page }

            div {
                style: r#"
                    flex: 1;
                    padding: 20px;
                    overflow-y: auto;
                    background-color: #fff;
                "#,

                SuspenseBoundary {
                    fallback: |_| rsx! { Loading {} },
                    match *active_page.read() {
                        Page::Home => rsx! { Home {} },
                        Page::Performance => rsx! { Performance {} },
                        Page::Financials => rsx! { Financials {} },
                        Page::Options => rsx! { Options {} },
                        Page::Portfolio => rsx! { Portfolio {} },
                        Page::Screener => rsx! { Screener {} },
                        Page::News => rsx! { News {} },
                        Page::NotFound => rsx! { h1 { "404 Not Found" } },
                    }
                }
            }
        }
    }
}