# Alerts Log

Persistent record of all detected anomalies + human validation.
Each entry is appended by the scanner. Validation updated via POST /alerts/{id}/feedback.

Format:
- **Validation**: pending | ✓ valid | ✗ rejected — {notes}

---
