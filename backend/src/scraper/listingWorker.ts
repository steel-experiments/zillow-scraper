import {
  createSteelSession,
  releaseSteelSession,
  SteelSession,
} from "./steelClient";
import { ListingData } from "../schema/dynamicSchema";
import { JobLogger } from "../logging/logger";

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Try to extract listing data from Zillow's __NEXT_DATA__ JSON blob.
 * This contains all listing data without needing to interact with the DOM.
 */
function extractFromNextData(nextData: Record<string, unknown>): Record<string, unknown> | null {
  try {
    // Navigate the __NEXT_DATA__ structure to find property data
    const props = nextData?.props as Record<string, unknown> | undefined;
    const pageProps = props?.pageProps as Record<string, unknown> | undefined;

    // Zillow stores the property data under different keys depending on the page type
    const componentProps = pageProps?.componentProps as Record<string, unknown> | undefined;

    let property: Record<string, unknown> | null = null;

    // Helper: search a gdpClientCache (object with GraphQL query keys) for property data
    const findPropertyInCache = (cache: Record<string, unknown>): Record<string, unknown> | null => {
      for (const value of Object.values(cache)) {
        try {
          const entry = typeof value === "string" ? JSON.parse(value) : value;
          const prop = (entry as Record<string, unknown>)?.property;
          if (prop && typeof prop === "object") {
            return prop as Record<string, unknown>;
          }
        } catch {}
      }
      return null;
    };

    // Helper: resolve a gdpClientCache value which may be a JSON string or object
    const resolveCache = (raw: unknown): Record<string, unknown> | null => {
      if (!raw) return null;
      if (typeof raw === "string") {
        try { return JSON.parse(raw); } catch { return null; }
      }
      if (typeof raw === "object") return raw as Record<string, unknown>;
      return null;
    };

    // Strategy 1: componentProps.gdpClientCache (most common path)
    if (!property && componentProps?.gdpClientCache) {
      const cache = resolveCache(componentProps.gdpClientCache);
      if (cache) property = findPropertyInCache(cache);
    }

    // Strategy 2: pageProps.gdpClientCache
    if (!property && pageProps?.gdpClientCache) {
      const cache = resolveCache(pageProps.gdpClientCache);
      if (cache) property = findPropertyInCache(cache);
    }

    // Strategy 3: Direct initialData or similar top-level keys
    if (!property) {
      const initialData = pageProps?.initialData as Record<string, unknown> | undefined;
      if (initialData?.property) {
        property = initialData.property as Record<string, unknown>;
      }
    }

    if (!property) return null;

    const result: Record<string, unknown> = {};

    // === Address ===
    const addr = property.address as Record<string, unknown> | undefined;
    if (addr) {
      const parts = [addr.streetAddress, addr.city, addr.state, addr.zipcode].filter(Boolean);
      result.address = parts.join(", ") || null;
    } else {
      result.address = null;
    }

    // === Price ===
    result.price = property.price != null ? `$${Number(property.price).toLocaleString()}` : null;

    // === Zestimate ===
    result.zestimate = property.zestimate != null ? `$${Number(property.zestimate).toLocaleString()}` : null;

    // === Rent Zestimate ===
    result.rent_zestimate = property.rentZestimate != null
      ? `$${Number(property.rentZestimate).toLocaleString()}`
      : null;

    // === Estimated sales range ===
    const zestimateLow = property.zestimateLowPercent as number | undefined;
    const zestimateHigh = property.zestimateHighPercent as number | undefined;
    const zestimateVal = property.zestimate as number | undefined;
    if (zestimateVal && zestimateLow != null && zestimateHigh != null) {
      const low = Math.round(zestimateVal * (1 - zestimateLow / 100));
      const high = Math.round(zestimateVal * (1 + zestimateHigh / 100));
      result.estimated_sales_range = `$${low.toLocaleString()} - $${high.toLocaleString()}`;
    } else {
      result.estimated_sales_range = null;
    }

    // === Basic facts ===
    if (property.bedrooms != null) result.fact_bedrooms = String(property.bedrooms);
    if (property.bathrooms != null) result.fact_bathrooms = String(property.bathrooms);
    if (property.livingArea != null) result.fact_living_area = `${property.livingArea} sqft`;
    if (property.lotSize != null) result.fact_lot_size = `${property.lotSize} sqft`;
    if (property.lotAreaValue != null && property.lotAreaUnits != null) {
      result.fact_lot_size = `${property.lotAreaValue} ${property.lotAreaUnits}`;
    }
    if (property.yearBuilt != null) result.fact_year_built = String(property.yearBuilt);
    if (property.homeType) result.fact_type = String(property.homeType);
    if (property.homeStatus) result.fact_status = String(property.homeStatus);
    if (property.parkingCapacity != null) result.fact_parking = String(property.parkingCapacity);

    // === Heating / Cooling ===
    if (property.heatingSystem) result.fact_heating = String(property.heatingSystem);
    if (property.coolingSystem) result.fact_cooling = String(property.coolingSystem);

    // === Facts & Features from resoFacts ===
    const resoFacts = property.resoFacts as Record<string, unknown> | undefined;
    if (resoFacts) {
      const factMappings: Record<string, string> = {
        atAGlanceFacts: "", // array of {factLabel, factValue}
        bedrooms: "bedrooms",
        bathrooms: "bathrooms",
        bathroomsFull: "bathrooms_full",
        bathroomsHalf: "bathrooms_half",
        livingArea: "living_area",
        stories: "stories",
        homeType: "type",
        yearBuilt: "year_built",
        heating: "heating",
        cooling: "cooling",
        parking: "parking",
        parkingCapacity: "parking_capacity",
        garageSpaces: "garage_spaces",
        hasGarage: "has_garage",
        laundryFeatures: "laundry",
        appliances: "appliances",
        flooring: "flooring",
        basement: "basement",
        roofType: "roof",
        exteriorFeatures: "exterior_features",
        constructionMaterials: "construction",
        foundationDetails: "foundation",
        sewer: "sewer",
        waterSource: "water_source",
        architecturalStyle: "architectural_style",
        communityFeatures: "community_features",
        associationFee: "hoa_fee",
        associationFeeFrequency: "hoa_frequency",
      };

      for (const [jsonKey, factKey] of Object.entries(factMappings)) {
        if (jsonKey === "atAGlanceFacts") continue; // handled separately
        const val = resoFacts[jsonKey];
        if (val != null && val !== "" && val !== "None") {
          const strVal = Array.isArray(val) ? val.join(", ") : String(val);
          if (!result[`fact_${factKey}`]) {
            result[`fact_${factKey}`] = strVal;
          }
        }
      }

      // atAGlanceFacts is an array of {factLabel, factValue}
      const atAGlance = resoFacts.atAGlanceFacts as Array<{ factLabel: string; factValue: string }> | undefined;
      if (Array.isArray(atAGlance)) {
        for (const fact of atAGlance) {
          if (fact.factLabel && fact.factValue && fact.factValue !== "No Data") {
            const key = fact.factLabel.toLowerCase().replace(/[\s/]+/g, "_").replace(/[^a-z0-9_]/g, "");
            if (!result[`fact_${key}`]) {
              result[`fact_${key}`] = fact.factValue;
            }
          }
        }
      }
    }

    // === Price History ===
    const priceHistory = property.priceHistory as Array<Record<string, unknown>> | undefined;
    if (Array.isArray(priceHistory) && priceHistory.length > 0) {
      result.price_history = priceHistory.map((entry) => [
        entry.date || null,
        entry.event || null,
        entry.price != null ? `$${Number(entry.price).toLocaleString()}` : null,
        entry.source || null,
      ]);
    } else {
      result.price_history = null;
    }

    // === Tax History ===
    const taxHistory = property.taxHistory as Array<Record<string, unknown>> | undefined;
    if (Array.isArray(taxHistory) && taxHistory.length > 0) {
      result.public_tax_history = taxHistory.map((entry) => [
        entry.time || null,
        entry.taxPaid != null ? `$${Number(entry.taxPaid).toLocaleString()}` : null,
        entry.value != null ? `$${Number(entry.value).toLocaleString()}` : null,
      ]);
    } else {
      result.public_tax_history = null;
    }

    return result;
  } catch {
    return null;
  }
}

/** Scrape a single Zillow listing page and extract all available data. */
export async function scrapeListingPage(
  url: string,
  logger: JobLogger
): Promise<ListingData> {
  let session: SteelSession | null = null;
  try {
    session = await createSteelSession();
    const { page } = session;

    logger.info(`Navigating to listing: ${url}`);
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await wait(3000);

    // === Primary strategy: Extract from __NEXT_DATA__ JSON ===
    const nextDataResult = await page.evaluate(() => {
      const script = document.querySelector("script#__NEXT_DATA__");
      if (!script?.textContent) return null;
      try {
        return JSON.parse(script.textContent);
      } catch {
        return null;
      }
    });

    if (nextDataResult) {
      const extracted = extractFromNextData(nextDataResult);
      if (extracted && extracted.address) {
        logger.info(`Extracted data from __NEXT_DATA__ for: ${extracted.address}`);
        extracted.link = url;
        return extracted as ListingData;
      }
    }

    // === Fallback: DOM scraping ===
    logger.info("__NEXT_DATA__ extraction failed, falling back to DOM scraping...");

    // Scroll down to trigger lazy loading of facts, history, etc.
    await page.evaluate(async () => {
      for (let i = 0; i < 8; i++) {
        window.scrollBy(0, 1000);
        await new Promise((r) => setTimeout(r, 400));
      }
      window.scrollTo(0, 0);
    });
    await wait(2000);

    // Try to click "See more facts and features" and expand buttons
    try {
      const buttons = await page.$$("button");
      for (const btn of buttons) {
        const text = await btn.evaluate((el) => el.textContent?.trim() ?? "");
        if (
          /see (more|all|complete)/i.test(text) ||
          /show more/i.test(text) ||
          /see full/i.test(text)
        ) {
          try {
            await btn.click();
            await wait(500);
          } catch {}
        }
      }
    } catch {}

    await wait(1000);

    const data = await page.evaluate(() => {
      const result: Record<string, unknown> = {};
      const bodyText = document.body.innerText;

      // === Address ===
      const addressEl =
        document.querySelector("h1") ??
        document.querySelector('[data-testid="bdp-property-address"]');
      result.address = addressEl?.textContent?.trim() ?? null;

      // === Price ===
      const priceEl = document.querySelector(
        '[data-testid="price"], span[class*="StyledPrice" i]'
      );
      if (priceEl) {
        result.price = priceEl.textContent?.trim() ?? null;
      } else {
        const match = bodyText.match(/\$[\d,]+(?:,\d{3})+/);
        result.price = match ? match[0] : null;
      }

      // === Zestimate ===
      const zestMatch = bodyText.match(/\$([\d,]+)\s*Zestimate/i);
      result.zestimate = zestMatch ? `$${zestMatch[1]}` : null;

      // === Estimated sales range ===
      const rangeMatch = bodyText.match(
        /Estimated\s+sale(?:s)?\s+range[:\s]*(\$[\d,]+\s*[-–]\s*\$[\d,]+)/i
      );
      result.estimated_sales_range = rangeMatch ? rangeMatch[1].trim() : null;

      // === Rent Zestimate ===
      const rentMatch = bodyText.match(/Rent\s+Zestimate[®:\s]*\$([\d,]+)/i);
      result.rent_zestimate = rentMatch ? `$${rentMatch[1]}` : null;

      // === Facts & Features ===
      const factSelectors = [
        '[class*="fact" i] li',
        '[data-testid*="fact"] li',
        ".data-view-container li",
      ];

      const seenFacts = new Set<string>();
      for (const sel of factSelectors) {
        document.querySelectorAll(sel).forEach((li) => {
          const text = li.textContent?.trim() ?? "";
          if (text.includes(":") && !seenFacts.has(text)) {
            seenFacts.add(text);
            const colonIdx = text.indexOf(":");
            const rawKey = text.slice(0, colonIdx).trim();
            const val = text.slice(colonIdx + 1).trim();
            if (rawKey && val && rawKey.length < 60) {
              const key = rawKey.toLowerCase().replace(/[\s/]+/g, "_").replace(/[^a-z0-9_]/g, "");
              result[`fact_${key}`] = val;
            }
          }
        });
      }

      // Also extract from h4/h5 section headers + their sibling lists
      document.querySelectorAll("h4, h5, h6").forEach((h) => {
        const sectionTitle = h.textContent?.trim() ?? "";
        const factSections = [
          "bedrooms", "bathrooms", "parking", "type", "style", "condition",
          "interior", "exterior", "heating", "cooling", "appliances",
          "flooring", "property", "lot", "construction", "utilities",
          "community", "hoa", "financial", "other",
        ];
        const isFactSection = factSections.some((s) =>
          sectionTitle.toLowerCase().includes(s)
        );
        if (!isFactSection) return;

        const nextEl = h.nextElementSibling;
        if (!nextEl) return;
        nextEl.querySelectorAll("li").forEach((li) => {
          const text = li.textContent?.trim() ?? "";
          if (text.includes(":") && !seenFacts.has(text)) {
            seenFacts.add(text);
            const colonIdx = text.indexOf(":");
            const rawKey = text.slice(0, colonIdx).trim();
            const val = text.slice(colonIdx + 1).trim();
            if (rawKey && val && rawKey.length < 60) {
              const key = rawKey.toLowerCase().replace(/[\s/]+/g, "_").replace(/[^a-z0-9_]/g, "");
              result[`fact_${key}`] = val;
            }
          }
        });
      });

      // === Price History ===
      const priceHistoryRows: unknown[] = [];
      document.querySelectorAll("table").forEach((table) => {
        const section = table.closest("section, div");
        const header = section?.querySelector("h2, h3, h4, h5");
        if (header && /price\s*history/i.test(header.textContent ?? "")) {
          table.querySelectorAll("tbody tr").forEach((tr) => {
            const cells = tr.querySelectorAll("td");
            if (cells.length >= 3) {
              priceHistoryRows.push([
                cells[0].textContent?.trim(),
                cells[1].textContent?.trim(),
                cells[2].textContent?.trim(),
              ]);
            }
          });
        }
      });
      result.price_history = priceHistoryRows.length > 0 ? priceHistoryRows : null;

      // === Public Tax History ===
      const taxRows: unknown[] = [];
      document.querySelectorAll("table").forEach((table) => {
        const section = table.closest("section, div");
        const header = section?.querySelector("h2, h3, h4, h5");
        if (header && /tax\s*history/i.test(header.textContent ?? "")) {
          table.querySelectorAll("tbody tr").forEach((tr) => {
            const cells = tr.querySelectorAll("td");
            if (cells.length >= 3) {
              taxRows.push([
                cells[0].textContent?.trim(),
                cells[1].textContent?.trim(),
                cells[2].textContent?.trim(),
              ]);
            }
          });
        }
      });
      result.public_tax_history = taxRows.length > 0 ? taxRows : null;

      return result;
    });

    data.link = url;
    return data as ListingData;
  } finally {
    if (session) await releaseSteelSession(session);
  }
}
