function validateField(box, isValid) {
	switch (isValid) {
		case "blank":
			box.parentNode.classList.remove("has-success");
			box.parentNode.classList.remove("has-error");
			break;
		case true:
			box.parentNode.classList.add("has-success");
			box.parentNode.classList.remove("has-error");
			break;
		case false:
			box.parentNode.classList.add("has-error");
			box.parentNode.classList.remove("has-success");
			break;
	}
}

function validateForm(formId, bounds) {
	var textfields = document.getElementById(formId).children;	// textfields[i].children[0] is a text box
	for (i = 0; i < textfields.length; i++) {
		var box = textfields[i].children[0];
		if (!box.value) {
			validateField(box, "blank");
		} else {
			switch (bounds[i][0]) {						// switch on type of object (date, int, float)
				case "date":
					validateField(box, (new Date(box.value) != "Invalid Date"));
					validateField(box, isValid);
					break;
				case "int":
					var min = bounds[i][1];
					var max = bounds[i][2];
					var value = box.value;
					var isValid = (Math.floor(value) == value && value >= min && value <= (max - 1));
					validateField(box, isValid);
					break;
				case "float":
					var min = bounds[i][1];
					var max = bounds[i][2];
					var value = box.value;
					validateField(box, !isNaN(box.value) && value >= min && value <= max);
					break;
			}
		}
	}
}

function validate() {
	var hqValid = validateHQuery();
	var boundsValid = validateBounds();
	document.getElementById('submit').disabled = (!hqValid || !boundsValid);
}


function validateHQuery() {
	var formId = 'hquery';
	var bounds = [['date', 0, 0], ['int', 0, 24], ['int', 0, 60], ['int', 0, 60]];
	validateForm(formId, bounds);
	return ((	!document.getElementById('hqToggle').children[0].checked)								// HQuery disabled
			||	(document.getElementById('calendar').parentNode.classList.contains("has-success")		// OR (Date valid
				&& !document.getElementById('ts_hours').parentNode.classList.contains("has-error")
				&& !document.getElementById('ts_minutes').parentNode.classList.contains("has-error")
				&& !document.getElementById('ts_seconds').parentNode.classList.contains("has-error")))	// AND rest not invalid)
}

function validateBounds() {
	var formId = 'custBounds';
	var bounds = [['float', -180, 180], ['float', -180, 180], ['float', -90, 90], ['float', -90, 90]];
	validateForm(formId, bounds);
	return ((	!document.getElementById('custToggle').children[0].checked)								// Custom Bounds disabled
			||	(document.getElementById('lBox').parentNode.classList.contains("has-success")
				&& document.getElementById('rBox').parentNode.classList.contains("has-success")
				&& document.getElementById('tBox').parentNode.classList.contains("has-success")
				&& document.getElementById('bBox').parentNode.classList.contains("has-success")))		// OR all fields valid
}

function toggle(checkboxId, formId) {
	var checkbox = document.getElementById(checkboxId).children[0];
	var form = document.getElementById(formId);
	if (checkbox.checked) {
		form.style.display = "";
	} else {
		form.style.display = "none";
		for (i = 0; i < form.childElementCount; i++) {
			form.children[i].children[0].value = "";
			validateField(form.children[i].children[0], "blank");
		}
	}
	validate();
}

