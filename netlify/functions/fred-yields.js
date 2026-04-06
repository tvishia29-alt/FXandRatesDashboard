exports.handler = async () => {
  try {
    const key = process.env.FRED_API_KEY;
    if (!key) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing FRED_API_KEY" })
      };
    }

    const seriesMap = {
      y2: "DGS2",
      y10: "DGS10",
      y30: "DGS30",
      y3m: "DGS3MO"
    };

    const entries = await Promise.all(
      Object.entries(seriesMap).map(async ([label, seriesId]) => {
        const url =
          "https://api.stlouisfed.org/fred/series/observations" +
          `?series_id=${seriesId}` +
          `&api_key=${key}` +
          "&file_type=json&sort_order=desc&limit=10";

        const res = await fetch(url);
        const data = await res.json();

        if (!res.ok) {
          throw new Error(data?.error_message || `FRED failed for ${seriesId}`);
        }

        const obs = (data.observations || []).find(x => x.value !== ".");
        return [label, obs ? Number(obs.value) : null];
      })
    );

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "public, max-age=300"
      },
      body: JSON.stringify(Object.fromEntries(entries))
    };
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message || "fred-yields failed" })
    };
  }
};
