function handleFormSubmit(event) {
  event.preventDefault(); // Prevent default form submission

  const form = event; // The form that triggered the event
  const type = event.submitter.dataset.type; // The type of button that triggered the event

  const formData = new FormData(form.target); // Create a new FormData object
  formData.append("type", type); // Add the type to the form data

  // Process data based on the form and item ID

  fetch("/submit", {
    method: "POST",
    body: formData,
  })
    .then((response) => response.json())
    .then((data) => {
      console.log("Success!", data);
      var card = event.target.parentNode;
      console.log(card.parentNode); // The form that triggered the event
      card.style.opacity = "0.5"; // Gray out the row
      var container = card.parentNode;
      container.appendChild(card); // Move row to the bottom
    })
    .catch((error) => {
      console.error("Error:", error);
    });
}

// Attach the same function to all forms
document.querySelectorAll("form").forEach((form) => {
  form.addEventListener("submit", handleFormSubmit);
});
