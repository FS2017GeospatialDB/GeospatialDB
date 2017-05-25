		function validateDate(id) {
			var box = document.getElementById(id);
			var date = new Date(box.value);
			if (!box.value) {
				box.parentNode.classList.remove("has-success");
				box.parentNode.classList.remove("has-error");
			} else if (date != "Invalid Date") {
				box.parentNode.classList.add("has-success");
				box.parentNode.classList.remove("has-error");
			} else {
				box.parentNode.classList.add("has-error");
				box.parentNode.classList.remove("has-success");
			}
			validateForm();
		}
		function validate(id, min, max) {
			var box = document.getElementById(id);
			var hour = Number(box.value);
			if (!box.value) {
				box.parentNode.classList.remove("has-success");
				box.parentNode.classList.remove("has-error");
			} else if (Math.floor(hour) == hour && hour >= min && hour <= (max - 1)) {
				box.parentNode.classList.add("has-success");
				box.parentNode.classList.remove("has-error");
			} else {
				box.parentNode.classList.add("has-error");
				box.parentNode.classList.remove("has-success");
			}
			validateForm();
		}

		function validateForm() {
			date = document.getElementById('calendar');
			hours = document.getElementById('ts_hours');
			minutes = document.getElementById('ts_minutes');
			seconds = document.getElementById('ts_seconds');
			if (!document.getElementById('hqToggle').children[0].checked) {
					document.getElementById('submit').disabled = false;
					return;
			}
			if (date.parentNode.classList.contains("has-success")) {
				if (!hours.parentNode.classList.contains("has-error") && !minutes.parentNode.classList.contains("has-error") && !seconds.parentNode.classList.contains("has-error")) {
					document.getElementById('submit').disabled = false;
				}
				else {
					document.getElementById('submit').disabled = true;
				}
			}
			else {
				document.getElementById('submit').disabled = true;
			}
		}

		function actHist() {
			checkbox = document.getElementById('hqToggle').children[0];
			cal = document.getElementById('calendar');
			hours = document.getElementById('ts_hours');
			min = document.getElementById('ts_minutes');
			sec = document.getElementById('ts_seconds');
			if (checkbox.checked) {
				cal.disabled = hours.disabled = min.disabled = sec.disabled = false;
			} else {
				cal.disabled = hours.disabled = min.disabled = sec.disabled = true;
				calendar.value = hours.value = min.value = sec.value = "";
				validateDate('calendar');
				validate('ts_hours', 0, 24);
				validate('ts_minutes', 0, 60);
				validate('ts_seconds', 0, 60);
			}
		}
