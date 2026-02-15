import { Page } from "puppeteer-core";
import { JobLogger } from "../logging/logger";

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Try to extract listing URLs from Zillow's embedded JSON data (__NEXT_DATA__ or preloaded state).
 * This is more reliable than DOM scraping since it doesn't depend on rendering.
 */
async function extractUrlsFromPageData(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const urls: string[] = [];
    const seen = new Set<string>();

    // Strategy 1: Extract from __NEXT_DATA__ script tag
    const nextDataScript = document.querySelector('script#__NEXT_DATA__');
    if (nextDataScript) {
      try {
        const json = JSON.parse(nextDataScript.textContent || "{}");
        const str = JSON.stringify(json);
        const matches = str.matchAll(/\/homedetails\/[^"?]+_zpid\//g);
        for (const m of matches) {
          const url = `https://www.zillow.com${m[0]}`;
          const zpid = m[0].match(/(\d+)_zpid/)?.[1];
          if (zpid && !seen.has(zpid)) {
            seen.add(zpid);
            urls.push(url);
          }
        }
      } catch {}
    }

    // Strategy 2: Search all script tags for preloaded search results data
    if (urls.length === 0) {
      const scripts = Array.from(document.querySelectorAll("script"));
      for (const script of scripts) {
        const text = script.textContent || "";
        if (text.includes("/homedetails/") && text.includes("_zpid")) {
          const matches = text.matchAll(/\/homedetails\/[^"'\\?]+_zpid\//g);
          for (const m of matches) {
            const url = `https://www.zillow.com${m[0]}`;
            const zpid = m[0].match(/(\d+)_zpid/)?.[1];
            if (zpid && !seen.has(zpid)) {
              seen.add(zpid);
              urls.push(url);
            }
          }
        }
      }
    }

    return urls;
  });
}

/**
 * Extract listing URLs from a Zillow search results page.
 * Uses both embedded JSON data and DOM links for maximum coverage.
 */
export async function extractListingUrls(page: Page, logger?: JobLogger): Promise<string[]> {
  const seen = new Set<string>();
  const allUrls: string[] = [];

  const addUrl = (url: string) => {
    const zpid = url.match(/(\d+)_zpid/)?.[1];
    const key = zpid || url;
    if (!seen.has(key)) {
      seen.add(key);
      allUrls.push(url.split("?")[0]);
    }
  };

  // Try extracting from embedded JSON data first (most reliable)
  const jsonUrls = await extractUrlsFromPageData(page);
  if (logger) logger.info(`Found ${jsonUrls.length} listings from page data.`);
  jsonUrls.forEach(addUrl);

  // Also try DOM-based extraction as fallback
  await page.evaluate(async () => {
    const container = document.querySelector('[id*="search-page-list"]') ?? document.documentElement;
    for (let i = 0; i < 5; i++) {
      container.scrollBy(0, 800);
      await new Promise((r) => setTimeout(r, 600));
    }
    container.scrollTop = 0;
  });
  await wait(2000);

  const domUrls = await page.evaluate(() => {
    const links: string[] = [];
    document.querySelectorAll('a[href*="/homedetails/"]').forEach((a) => {
      links.push((a as HTMLAnchorElement).href);
    });
    return links;
  });
  domUrls.forEach(addUrl);

  return allUrls;
}

/**
 * Build the next page URL from the current search URL and page number.
 */
export function buildNextPageUrl(searchUrl: string, nextPage: number): string {
  // Remove any existing page parameter
  const withoutPage = searchUrl.replace(/\d+_p\//, "");
  return withoutPage.replace(/\/$/, `/${nextPage}_p/`);
}

/**
 * Navigate a fresh page to the given URL and verify listings exist.
 * Called by the orchestrator after creating a fresh session for the next page.
 * Returns true if navigation succeeded and listings were found.
 */
export async function goToNextPage(
  page: Page,
  targetUrl: string,
  logger: JobLogger
): Promise<boolean> {
  try {
    logger.info(`Navigating to next page: ${targetUrl}`);
    await page.goto(targetUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await wait(3000);

    // Check for listings using both JSON data and DOM
    const urls = await extractListingUrls(page, logger);
    if (urls.length > 0) {
      logger.info(`Page loaded successfully with ${urls.length} listings.`);
      return true;
    }

    // Debug: log page title and URL to help diagnose what Zillow served
    const pageTitle = await page.title();
    const finalUrl = page.url();
    logger.warn(`No listings found. Page title: "${pageTitle}", URL: ${finalUrl}`);

    // Try reload once as Zillow sometimes serves empty on first load
    logger.info("Reloading page as final attempt...");
    await page.reload({ waitUntil: "networkidle2", timeout: 60_000 });
    await wait(3000);

    const retryUrls = await extractListingUrls(page, logger);
    if (retryUrls.length > 0) {
      logger.info(`After reload, found ${retryUrls.length} listings.`);
      return true;
    }

    logger.info("Next page has no listings — end of results.");
    return false;
  } catch (err) {
    logger.warn(`Failed to navigate to next page: ${err}`);
    return false;
  }
}
