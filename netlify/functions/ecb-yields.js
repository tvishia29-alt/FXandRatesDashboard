exports.handler = async () => {
  try {
    const seriesMap = {
      y2: "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
      y5: "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_5Y",
      y10: "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y"
    };

    const entries = await Promise.all(
      Object.entries(seriesMap).map(async ([label, key]) => {
        const url =
          `https://data-api.ecb.europa.eu/service/data/YC/${key}` +
          `?format=jsondata&lastNObservations=1&detail=dataonly`;

        const res = await fetch(url, {
          headers: {
            Accept: "application/json"
          }
        });

        const data = await res.json();

        if (!res.ok) {
          throw new Error(`ECB failed for ${label}`);
        }

        // ECB JSON can vary by structure; this safely hunts the last numeric observation.
        let value = null;

        try {
          const sets = data?.dataSets?.[0];
          const series = sets?.series;
          if (series) {
            const firstSeries = Object.values(series)[0];
            const obs = firstSeries?.observations;
            if (obs) {
              const firstObs = Object.values(obs)[0];
              value = Array.isArray(firstObs) ? Number(firstObs[0]) : Number(firstObs);
            }
          }
        } catch {}

        return [label, Number.isFinite(value) ? value : null];
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
      body: JSON.stringify({ error: err.message || "ecb-yields failed" })
    };
  }
};
