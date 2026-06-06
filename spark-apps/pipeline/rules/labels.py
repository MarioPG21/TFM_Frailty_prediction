def get_label_rules() -> list[dict]:
    """
    Reglas de calidad para el flujo de confirmación clínica diferida (labels).

    La regla más importante es temporal_coherence: garantiza que la etiqueta
    siempre se conoce DESPUÉS de la medición clínica. Cualquier registro que
    la viole va a cuarentena antes de llegar a Gold, protegiendo el pipeline
    contra data leakage.
    """
    return [
        # Identidad
        {"name": "valid_patient_id_lbl",
         "constraint": "patient_id IS NOT NULL",
         "tag": "labels"},
        {"name": "valid_snapshot_date_lbl",
         "constraint": "snapshot_date IS NOT NULL",
         "tag": "labels"},
        {"name": "valid_label_available_date",
         "constraint": "label_available_date IS NOT NULL",
         "tag": "labels"},

        # Dominio de la etiqueta
        {"name": "valid_frailty_label_lbl",
         "constraint": "frailty_label IN (0, 1)",
         "tag": "labels"},

        # Coherencia temporal: la confirmación diagnóstica SIEMPRE es posterior
        # a la fecha del snapshot de mediciones.
        {"name": "temporal_coherence",
         "constraint": "label_available_date > snapshot_date",
         "tag": "labels"},
    ]
