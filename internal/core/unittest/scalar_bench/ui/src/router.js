const routes = [];

export function registerRoute(matcher, handler) {
  routes.push({ matcher, handler });
}

export function initRouter() {
  window.addEventListener('hashchange', handleRoute);
  handleRoute();
}

function handleRoute() {
  const hash = window.location.hash || '#/runs';
  for (const route of routes) {
    const match = route.matcher(hash);
    if (match) {
      route.handler(match.params || {});
      highlightNav(hash);
      return;
    }
  }
  window.location.hash = '#/runs';
}

function highlightNav(hash) {
  document.querySelectorAll('header nav a').forEach((link) => {
    if (!hash && link.hash === '#/runs') {
      link.classList.add('active');
      return;
    }
    if (link.hash === hash) {
      link.classList.add('active');
    } else {
      link.classList.remove('active');
    }
  });
}

export function navigateTo(hash) {
  if (window.location.hash === hash) {
    handleRoute();
  } else {
    window.location.hash = hash;
  }
}