// static/js/main.js

document.addEventListener('DOMContentLoaded', () => {
    initAutoHideAlerts();
    initBootstrapTooltips();
    initSearchForm();
    initBackToTop();
});

/**
 * Oculta automáticamente las alertas de Bootstrap.
 * - Si el alert tiene data-autoclose="false" no se cierra.
 * - Puedes controlar el tiempo con data-autoclose-delay="8000" (en ms).
 */
function initAutoHideAlerts() {
    const alerts = document.querySelectorAll('.alert');

    if (!alerts.length || !window.bootstrap) return;

    alerts.forEach(alert => {
        const autoCloseAttr = alert.getAttribute('data-autoclose');
        const shouldAutoClose = autoCloseAttr !== 'false'; // por defecto SÍ se autocierra

        if (!shouldAutoClose) return;

        const delayAttr = alert.getAttribute('data-autoclose-delay');
        const delay = delayAttr ? parseInt(delayAttr, 10) : 5000;

        setTimeout(() => {
            try {
                const bsAlert = bootstrap.Alert.getOrCreateInstance(alert);
                bsAlert.close();
            } catch (e) {
                console.error('Error cerrando alerta:', e);
            }
        }, delay);
    });
}

/**
 * Inicializa tooltips de Bootstrap para cualquier elemento
 * con data-bs-toggle="tooltip"
 */
function initBootstrapTooltips() {
    if (!window.bootstrap || !bootstrap.Tooltip) return;

    const tooltipTriggerList = [].slice.call(
        document.querySelectorAll('[data-bs-toggle="tooltip"]')
    );

    tooltipTriggerList.forEach(el => {
        try {
            new bootstrap.Tooltip(el);
        } catch (e) {
            console.error('Error iniciando tooltip:', e);
        }
    });
}

/**
 * Lógica básica para el formulario de búsqueda de la landing:
 * - Evita enviar si el texto está vacío.
 * - Muestra estado de "cargando" en el botón.
 *
 * Estructura sugerida en el HTML:
 *   <form id="formBusqueda">
 *       <input id="inputBusqueda" ...>
 *       <button id="btnBuscar" type="submit">Buscar</button>
 *   </form>
 */
function initSearchForm() {
    const form = document.getElementById('formBusqueda');
    const input = document.getElementById('inputBusqueda');
    const btn = document.getElementById('btnBuscar');

    if (!form || !input || !btn) return; // si no existe, no hacemos nada

    form.addEventListener('submit', (e) => {
        const query = input.value.trim();

        if (!query) {
            e.preventDefault();
            input.classList.add('is-invalid');

            // Opcional: mostrar un mensaje pequeño debajo
            let feedback = form.querySelector('.invalid-feedback');
            if (!feedback) {
                feedback = document.createElement('div');
                feedback.className = 'invalid-feedback';
                feedback.textContent = 'Por favor ingresa un término de búsqueda.';
                input.insertAdjacentElement('afterend', feedback);
            }
            return;
        }

        input.classList.remove('is-invalid');

        // Estado "cargando" en el botón
        btn.disabled = true;
        const originalText = btn.dataset.originalText || btn.textContent;
        btn.dataset.originalText = originalText;
        btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>Buscando...`;
    });
}

/**
 * Botón "volver arriba".
 * En el HTML:
 *   <button id="btnBackToTop" class="btn btn-sm ...">↑</button>
 */
function initBackToTop() {
    const btn = document.getElementById('btnBackToTop');
    if (!btn) return;

    const toggleVisibility = () => {
        if (window.scrollY > 200) {
            btn.classList.add('show');
        } else {
            btn.classList.remove('show');
        }
    };

    window.addEventListener('scroll', toggleVisibility);
    toggleVisibility();

    btn.addEventListener('click', () => {
        window.scrollTo({ top: 0, behavior: 'smooth' });
    });
}
