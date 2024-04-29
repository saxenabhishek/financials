function toggleQueryParam(paramName, value) {
  console.log("toggleQueryParam", paramName, value);
  const url = new URL(window.location.href);
  const params = new URLSearchParams(url.search);

  if (params.has(paramName) && params.get(paramName) == value) {
    params.delete(paramName);
  } else {
    // Param doesn't exist, add it
    params.set(paramName, value);
  }
  url.search = params.toString();
  window.location.href = url.toString();
}
