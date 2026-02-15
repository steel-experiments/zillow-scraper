import {
  createSteelSession,
  releaseSteelSession,
  SteelSession,
} from "./steelClient";
import { extractListingUrls, goToNextPage, buildNextPageUrl } from "./pageController";
import { scrapeListingPage } from "./listingWorker";
import { DynamicSchema } from "../schema/dynamicSchema";
import { JobLogger } from "../logging/logger";

const MAX_LISTINGS = 100;
const BATCH_CONCURRENCY = 5;
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

export interface ScrapeJob {
  id: string;
  status: "running" | "completed" | "error";
  schema: DynamicSchema;
  logger: JobLogger;
  abortController: AbortController;
}

/** Global registry of scrape jobs */
const jobs = new Map<string, ScrapeJob>();

export function getJob(id: string): ScrapeJob | undefined {
  return jobs.get(id);
}

/**
 * Main scraping orchestrator.
 * 1. Navigate to Zillow search URL.
 * 2. Extract listing URLs from search results page.
 * 3. Spawn concurrent workers for each listing (up to remaining count).
 * 4. Track global counter; stop at MAX_LISTINGS.
 * 5. Paginate if needed (fresh session per page).
 */
export async function startScrapeJob(
  jobId: string,
  searchUrl: string
): Promise<ScrapeJob> {
  const schema = new DynamicSchema();
  const logger = new JobLogger();
  const abortController = new AbortController();

  const job: ScrapeJob = {
    id: jobId,
    status: "running",
    schema,
    logger,
    abortController,
  };
  jobs.set(jobId, job);

  // Run orchestration in background
  orchestrate(job, searchUrl).catch((err) => {
    logger.error(`Orchestrator fatal error: ${err}`);
    job.status = "error";
  });

  return job;
}

async function orchestrate(job: ScrapeJob, searchUrl: string): Promise<void> {
  const { schema, logger, abortController } = job;
  let globalCount = 0;

  let mainSession: SteelSession | null = null;

  try {
    logger.info("Starting scrape job...");
    logger.info(`Search URL: ${searchUrl}`);

    // Open main browser session for search results navigation
    mainSession = await createSteelSession();
    const { page } = mainSession;

    logger.info("Navigating to Zillow search results...");
    await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await wait(3000);

    let pageNum = 1;

    while (globalCount < MAX_LISTINGS) {
      if (abortController.signal.aborted) break;

      logger.info(`--- Processing results page ${pageNum} ---`);

      const listingUrls = await extractListingUrls(mainSession.page, logger);
      logger.info(
        `Found ${listingUrls.length} listings on page ${pageNum}.`
      );

      if (listingUrls.length === 0) {
        logger.warn("No listing URLs found on this page. Stopping.");
        break;
      }

      // Determine how many to scrape from this page (respect global limit)
      const remaining = MAX_LISTINGS - globalCount;
      const toScrape = listingUrls.slice(0, remaining);

      logger.info(
        `Scraping ${toScrape.length} listings (${globalCount}/${MAX_LISTINGS} done so far)...`
      );

      // Launch workers in batches of BATCH_CONCURRENCY
      const results = await scrapeListingsBatch(
        toScrape,
        logger,
        abortController,
        globalCount,
        MAX_LISTINGS
      );

      for (const result of results) {
        if (result.success && result.data) {
          schema.addRow(result.data);
          globalCount++;
          logger.scraped = globalCount;
          logger.success(
            `[${globalCount}/${MAX_LISTINGS}] Scraped: ${result.data.address || result.url}`
          );
        } else {
          logger.error(`Failed: ${result.url} — ${result.error}`);
        }

        if (globalCount >= MAX_LISTINGS) {
          logger.info("Reached 100 listing limit. Stopping.");
          break;
        }
      }

      if (globalCount >= MAX_LISTINGS) break;

      // Release the current main session before pagination
      logger.info("Releasing main session before navigating to next page...");
      await releaseSteelSession(mainSession);
      mainSession = null;

      // Create a fresh session for the next page (new proxy IP/fingerprint)
      const nextPageUrl = buildNextPageUrl(searchUrl, pageNum + 1);
      logger.info("Creating fresh session for next page...");
      mainSession = await createSteelSession();

      const hasNext = await goToNextPage(mainSession.page, nextPageUrl, logger);
      if (!hasNext) break;
      pageNum++;
    }

    logger.success(
      `Scraping complete. Total listings scraped: ${globalCount}`
    );
    job.status = "completed";
  } catch (err) {
    logger.error(`Orchestrator error: ${err}`);
    job.status = "error";
  } finally {
    if (mainSession) await releaseSteelSession(mainSession);
  }
}

interface ScrapeResult {
  url: string;
  success: boolean;
  data?: Record<string, unknown>;
  error?: string;
}

/**
 * Scrape a batch of listings with limited concurrency.
 * Processes workers in chunks of BATCH_CONCURRENCY to avoid
 * opening too many simultaneous Steel sessions.
 */
async function scrapeListingsBatch(
  urls: string[],
  logger: JobLogger,
  abortController: AbortController,
  currentCount: number,
  maxCount: number
): Promise<ScrapeResult[]> {
  let completed = currentCount;
  const allResults: ScrapeResult[] = [];

  // Process in chunks of BATCH_CONCURRENCY
  for (let i = 0; i < urls.length; i += BATCH_CONCURRENCY) {
    if (completed >= maxCount || abortController.signal.aborted) break;

    const chunk = urls.slice(i, i + BATCH_CONCURRENCY);
    logger.info(
      `Processing batch ${Math.floor(i / BATCH_CONCURRENCY) + 1} (${chunk.length} workers)...`
    );

    const promises = chunk.map(async (url, idx): Promise<ScrapeResult> => {
      const workerNum = i + idx + 1;
      if (completed >= maxCount || abortController.signal.aborted) {
        return { url, success: false, error: "Skipped — limit reached" };
      }

      logger.info(`[Worker ${workerNum}/${urls.length}] Starting: ${url}`);

      try {
        const data = await scrapeListingPage(url, logger);
        if (completed >= maxCount || abortController.signal.aborted) {
          return { url, success: false, error: "Skipped — limit reached mid-scrape" };
        }
        completed++;
        return { url, success: true, data };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error(`[Worker ${workerNum}] Error scraping ${url}: ${msg}`);
        return { url, success: false, error: msg };
      }
    });

    const settled = await Promise.allSettled(promises);
    for (const s of settled) {
      if (s.status === "fulfilled") {
        allResults.push(s.value);
      } else {
        allResults.push({
          url: "unknown",
          success: false,
          error: s.reason?.message ?? String(s.reason),
        });
      }
    }
  }

  return allResults;
}
