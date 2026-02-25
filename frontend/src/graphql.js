/**
 * Reusable GraphQL query/mutation helper.
 * Reads the endpoint from window.__CONFIG__.GRAPHQL_URL, falling back to localhost.
 *
 * @param {string} query  - GraphQL query or mutation string
 * @param {object} variables - Variables object
 * @returns {Promise<object>} Parsed `data` field from the GraphQL response
 */
export async function graphqlQuery(query, variables = {}) {
  const url =
    (typeof window !== "undefined" && window.__CONFIG__?.GRAPHQL_URL) ||
    "http://localhost:4000/graphql";

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });

  if (!res.ok) {
    throw new Error(`GraphQL HTTP error ${res.status}: ${await res.text()}`);
  }

  const json = await res.json();

  if (json.errors?.length) {
    throw new Error(json.errors.map((e) => e.message).join("; "));
  }

  return json.data;
}
