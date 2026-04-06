exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return {
      statusCode: 405,
      body: JSON.stringify({ error: "Method not allowed" }),
    };
  }

  try {
    const payload = JSON.parse(event.body || "{}");

    const systemPrompt = `
You are a senior FX and rates strategist writing a country deep-dive for a trading dashboard.

Rules:
- Use ONLY the data provided.
- Do not invent numbers, dates, or views.
- If something is missing, say "Not available from provided data".
- Write in concise institutional language.
- Focus on FX, rates, central bank path, inflation, growth, labour market, and risks.
- Keep it useful for a trader.
`;

    const userPrompt = `
Create a country deep-dive from this dashboard snapshot.

${JSON.stringify(payload, null, 2)}
`;

    const schema = {
      type: "object",
      additionalProperties: false,
      properties: {
        narrative: { type: "string" },
        macro_sections: {
          type: "array",
          items: {
            type: "object",
            additionalProperties: false,
            properties: {
              title: { type: "string" },
              bullets: {
                type: "array",
                items: { type: "string" }
              }
            },
            required: ["title", "bullets"]
          }
        },
        fx_view: {
          type: "object",
          additionalProperties: false,
          properties: {
            direction: { type: "string" },
            rationale: { type: "string" },
            levels_to_watch: {
              type: "array",
              items: { type: "string" }
            }
          },
          required: ["direction", "rationale", "levels_to_watch"]
        },
        rates_view: {
          type: "object",
          additionalProperties: false,
          properties: {
            bias: { type: "string" },
            rationale: { type: "string" }
          },
          required: ["bias", "rationale"]
        },
        key_risks: {
          type: "array",
          items: { type: "string" }
        },
        headline_takeaways: {
          type: "array",
          items: { type: "string" }
        }
      },
      required: [
        "narrative",
        "macro_sections",
        "fx_view",
        "rates_view",
        "key_risks",
        "headline_takeaways"
      ]
    };

    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: "gpt-5",
        reasoning: { effort: "low" },
        store: false,
        input: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt }
        ],
        text: {
          format: {
            type: "json_schema",
            strict: true,
            schema
          }
        }
      })
    });

    const data = await response.json();

    if (!response.ok) {
      return {
        statusCode: response.status,
        body: JSON.stringify({
          error: data?.error?.message || "OpenAI request failed"
        })
      };
    }

    const text = data.output?.[0]?.content?.[0]?.text;

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: text
    };
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: err.message || "Server error"
      })
    };
  }
};