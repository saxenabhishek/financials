function handleFormSubmit(event) {
  event.preventDefault(); // Prevent default form submission

  const form = event.target; // The form that triggered the event

  // Process data based on the form and item ID

  // Send data using AJAX (replace with your preferred library)
  fetch("/submit", {
    method: "POST",
    body: new FormData(form),
  })
    .then((response) => response.json())
    .then((data) => {
      console.log("Success!", data);
    })
    .catch((error) => {
      console.error("Error:", error);
    });
}

// Attach the same function to all forms
document.querySelectorAll("form").forEach((form) => {
  form.addEventListener("submit", handleFormSubmit);
});
