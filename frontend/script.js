const askBtn = document.getElementById("askBtn");
const questionInput = document.getElementById("question");
const answerEl = document.getElementById("answer");
const sqlEl = document.getElementById("sql");
const tableEl = document.getElementById("table");
const output = document.getElementById("output");

// ✅ Automatically detect backend URL (local vs Render)
const API_URL = window.location.origin.includes("onrender.com")
  ? `${window.location.origin}/ask` // e.g., https://crop-dcdl.onrender.com/ask
  : "http://127.0.0.1:8000/ask";    // for local testing

askBtn.addEventListener("click", async () => {
  const question = questionInput.value.trim();
  if (!question) {
    alert("Please enter a question first!");
    return;
  }

  output.style.display = "block";
  answerEl.textContent = "⏳ Thinking...";
  sqlEl.textContent = "";
  tableEl.innerHTML = "";

  try {
    const res = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
    });

    if (!res.ok) {
      let errMsg = `HTTP ${res.status}`;
      try {
        const errJson = await res.json();
        if (errJson && (errJson.detail || errJson.error)) {
          errMsg += ` - ${errJson.detail || errJson.error}`;
        }
      } catch {}
      throw new Error(errMsg);
    }

    const data = await res.json();

    // ✅ Display response
    answerEl.textContent = data.answer || "(no summary)";
    sqlEl.textContent = data.sql || "(no SQL returned)";

    // ✅ Render table dynamically
    if (data.rows && data.rows.length > 0) {
      const headers = Object.keys(data.rows[0]);
      const tableHTML = `
        <table>
          <thead><tr>${headers.map(h => `<th>${h}</th>`).join("")}</tr></thead>
          <tbody>
            ${data.rows.map(r => `<tr>${headers.map(h => `<td>${r[h]}</td>`).join("")}</tr>`).join("")}
          </tbody>
        </table>
      `;
      tableEl.innerHTML = tableHTML;
    } else {
      tableEl.innerHTML = "<p>No data found.</p>";
    }

  } catch (err) {
    console.error(err);
    answerEl.textContent = `❌ ${err.message || "Error connecting to backend."}`;
  }
});
