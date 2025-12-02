// static/js/main.js

document.addEventListener("DOMContentLoaded", () => {
  // =========================
  // 1. Año actual en el footer
  // =========================
  const yearSpan = document.getElementById("current-year");
  if (yearSpan) {
    yearSpan.textContent = new Date().getFullYear();
  }

  // =========================
  // 2. Lógica del BUSCADOR
  // =========================
  const form = document.getElementById("form-busqueda");
  const input = document.getElementById("texto_busqueda");
  const alerta = document.getElementById("alerta-busqueda");
  const resultadosSection = document.getElementById("resultados-section");
  const resultadosContainer = document.getElementById("resultados-container");
  const totalResultadosSpan = document.getElementById("total-resultados");

  // Si no estamos en la página del buscador, salimos
  if (!form || !input || !alerta || !resultadosSection || !resultadosContainer || !totalResultadosSpan) {
    return;
  }

  let ultimaBusqueda = [];

  function mostrarAlerta(mensaje, tipo = "info") {
    alerta.textContent = mensaje;
    alerta.className = "alert alert-" + tipo + " mx-auto";
    alerta.style.maxWidth = "980px";
  }

  function ocultarAlerta() {
    alerta.textContent = "";
    alerta.className = "alert d-none mx-auto";
  }

  function limpiarResultados() {
    resultadosContainer.innerHTML = "";
    resultadosSection.classList.add("d-none");
    totalResultadosSpan.textContent = "0";
  }

  // Intenta adaptar varios formatos posibles de respuesta
  function extraerHits(data) {
    if (Array.isArray(data.hits)) {
      return data.hits;
    }
    if (data.hits && Array.isArray(data.hits.hits)) {
      return data.hits.hits;
    }
    if (Array.isArray(data.resultados)) {
      return data.resultados;
    }
    return [];
  }

  function extraerTotal(data, hits) {
    if (typeof data.total === "number") {
      return data.total;
    }
    if (data.hits && typeof data.hits.total === "number") {
      return data.hits.total;
    }
    if (data.hits && data.hits.total && typeof data.hits.total.value === "number") {
      return data.hits.total.value;
    }
    return hits.length;
  }

  function renderResultados(data) {
    const hits = extraerHits(data);
    const total = extraerTotal(data, hits);

    ultimaBusqueda = hits;
    resultadosContainer.innerHTML = "";
    totalResultadosSpan.textContent = String(total);

    if (!hits.length) {
      resultadosContainer.innerHTML =
        '<div class="list-group-item text-center text-muted">No se encontraron resultados.</div>';
      resultadosSection.classList.remove("d-none");
      return;
    }

    hits.forEach((hit, index) => {
      const source = hit._source || hit.source || hit;

      const anio =
        source.anio ||
        source.año ||
        source.anio_publicacion ||
        source.fecha ||
        "";

      const tipo =
        source.tipo_norma ||
        source.tipo_documento ||
        source.tipo ||
        "";

      const entidad =
        source.entidad ||
        source.emisor ||
        source.organismo ||
        "";

      const titulo =
        source.titulo ||
        source.nombre_archivo ||
        "Documento sin título";

      const textoLargo =
        source.resumen ||
        source.extracto ||
        source.texto ||
        source.contenido ||
        "";

      let snippet = textoLargo || titulo;
      if (snippet.length > 260) {
        snippet = snippet.slice(0, 260) + "…";
      }

      const score = typeof hit._score === "number" ? hit._score.toFixed(4) : null;

      const item = document.createElement("div");
      item.className = "list-group-item list-group-item-action flex-column align-items-start";

      item.innerHTML = `
        <div class="d-flex w-100 justify-content-between">
          <div>
            <h5 class="mb-1">${titulo}</h5>
            <div class="small text-muted">
              ${anio ? `<span class="me-2"><strong>Año:</strong> ${anio}</span>` : ""}
              ${tipo ? `<span class="me-2"><strong>Tipo:</strong> ${tipo}</span>` : ""}
              ${entidad ? `<span class="me-2"><strong>Entidad:</strong> ${entidad}</span>` : ""}
            </div>
          </div>
          <div class="text-end">
            ${score !== null ? `<span class="badge bg-secondary">score ${score}</span>` : ""}
            <div class="small text-muted">#${index + 1}</div>
          </div>
        </div>
        <p class="mb-1 mt-2">${snippet}</p>
        <button type="button" class="btn btn-sm btn-outline-secondary mt-2 btn-detalle">
          Ver detalle
        </button>
        <div class="mt-2 d-none detalle-json">
          <pre class="bg-light border rounded p-2 small mb-0" style="max-height: 300px; overflow-y: auto;">
${JSON.stringify(source, null, 2)}
          </pre>
        </div>
      `;

      // Toggle del detalle JSON
      const btnDetalle = item.querySelector(".btn-detalle");
      const detalleDiv = item.querySelector(".detalle-json");

      btnDetalle.addEventListener("click", () => {
        const oculto = detalleDiv.classList.contains("d-none");
        if (oculto) {
          detalleDiv.classList.remove("d-none");
          btnDetalle.textContent = "Ocultar detalle";
        } else {
          detalleDiv.classList.add("d-none");
          btnDetalle.textContent = "Ver detalle";
        }
      });

      resultadosContainer.appendChild(item);
    });

    resultadosSection.classList.remove("d-none");
  }

  // =========================
  // 3. Submit del formulario
  // =========================
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const texto = (input.value || "").trim();

    if (!texto) {
      mostrarAlerta("Por favor ingresa un texto para buscar.", "warning");
      return;
    }

    limpiarResultados();
    mostrarAlerta("Buscando documentos... por favor espera.", "info");

    try {
      const resp = await fetch("/buscar-elastic", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          texto: texto,
          size: 20 // ajusta si quieres más/menos resultados
        })
      });

      if (!resp.ok) {
        mostrarAlerta(
          "Error al comunicarse con el servidor (HTTP " + resp.status + ").",
          "danger"
        );
        return;
      }

      const data = await resp.json();

      // Si el backend devuelve success=false en errores
      if (data.success === false) {
        mostrarAlerta(data.error || "Error en la búsqueda.", "danger");
        return;
      }

      ocultarAlerta();
      renderResultados(data);
    } catch (err) {
      console.error(err);
      mostrarAlerta("Error de comunicación con el servidor.", "danger");
    }
  });
});
