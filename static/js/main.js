// static/js/main.js

document.addEventListener("DOMContentLoaded", () => {
  // Año actual en el footer
  const yearSpan = document.getElementById("current-year");
  if (yearSpan) {
    yearSpan.textContent = new Date().getFullYear();
  }
});
