// static/js/main.js

document.addEventListener("DOMContentLoaded", () => {
  // Año actual en el footer
  const yearSpan = document.getElementById("current-year");
  if (yearSpan) {
    yearSpan.textContent = new Date().getFullYear();
  }

  // Lógica del formulario de búsqueda (buscador.html)
  const formBusqueda = document.getElementById("form-busqueda");
  if (formBusqueda) {
    formBusqueda.addEventListener("submit", async (e) => {
      e.preventDefault();

      const alerta = document.getElementById("alerta-busqueda");
      const resultadosSection = document.getElementById("resultados-section");
      const resultadosContainer = document.getElementById("resultados-container");
      const inputTexto = document.getElementById("texto_busqueda");

      if (!inputTexto) return;

      const query = inputTexto.value.trim();
      if (!query) {
        mostrarAlerta(alerta, "Debes escribir un texto de búsqueda.", "warning");
        return;
      }

      mostrarAlerta(alerta, "Buscando…", "info");
      resultadosSection.classList.add("d-none");
      resultadosContainer.innerHTML = "";

      try {
        const resp = await fetch("/buscar", {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({ q: query })
        });

        if (!resp.ok) {
          throw new Error("Respuesta no válida del servidor");
        }

        const data = await resp.json();

        if (!Array.isArray(data.resultados) || data.resultados.length === 0) {
          mostrarAlerta(alerta, "No se encontraron documentos para esa búsqueda.", "warning");
          return;
        }

        resultadosContainer.innerHTML = "";
        data.resultados.forEach((doc) => {
          const item = document.createElement("a");
          item.href = doc.url || "#";
          item.target = doc.url ? "_blank" : "_self";
          item.className = "list-group-item list-group-item-action";

          item.innerHTML = `
            <div class="d-flex w-100 justify-content-between">
              <h5 class="mb-1">${doc.titulo || "Documento sin título"}</h5>
              <small>${doc.fecha || ""}</small>
            </div>
            <p class="mb-1">${doc.resumen || ""}</p>
            <small>${doc.fuente || ""}</small>
          `;
          resultadosContainer.appendChild(item);
        });

        resultadosSection.classList.remove("d-none");
        mostrarAlerta(alerta, `Se encontraron ${data.resultados.length} documentos.`, "success");
      } catch (err) {
        console.error("Error en la búsqueda:", err);
        mostrarAlerta(alerta, "Error de comunicación con el servidor.", "danger");
      }
    });
  }
});

function mostrarAlerta(elemento, mensaje, tipo) {
  if (!elemento) return;
  elemento.textContent = "";
  elemento.className = "alert alert-" + tipo;
  elemento.textContent = mensaje;
  elemento.classList.remove("d-none");
}
