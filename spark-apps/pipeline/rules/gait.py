def get_gait_rules() -> list[dict]:
    """
    Reglas de calidad para la Fuente C (eventos de zancada).
    Rangos derivados de los parámetros gauss_clamp del generador.
    """
    return [
        # Identidad
        {"name": "valid_event_id",
         "constraint": "event_id IS NOT NULL",
         "tag": "gait"},
        {"name": "valid_patient_id_gait",
         "constraint": "patient_id IS NOT NULL",
         "tag": "gait"},
        {"name": "valid_session_id",
         "constraint": "session_id IS NOT NULL",
         "tag": "gait"},
        {"name": "valid_session_timestamp",
         "constraint": "session_timestamp IS NOT NULL",
         "tag": "gait"},

        # Marcha — gauss_clamp(0.55/0.70, 0.08, 0.2, 1.2)
        {"name": "valid_stride_length",
         "constraint": "stride_length_m >= 0.2 AND stride_length_m <= 1.2",
         "tag": "gait"},
        # gauss_clamp(1.3/0.95, 0.15, 0.5, 2.5)
        {"name": "valid_stride_time",
         "constraint": "stride_time_s >= 0.5 AND stride_time_s <= 2.5",
         "tag": "gait"},
        # gauss_clamp(85/105, 15, 40, 150)
        {"name": "valid_cadence",
         "constraint": "cadence_steps_min >= 40.0 AND cadence_steps_min <= 150.0",
         "tag": "gait"},
        # gauss_clamp(0.6/1.1, 0.25, 0.1, 2.0)
        {"name": "valid_gait_speed",
         "constraint": "gait_speed_m_s >= 0.1 AND gait_speed_m_s <= 2.0",
         "tag": "gait"},
        # gauss_clamp(0.12/0.05, 0.04, 0.0, 0.5)
        {"name": "valid_asymmetry_index",
         "constraint": "asymmetry_index >= 0.0 AND asymmetry_index <= 0.5",
         "tag": "gait"},
        # gauss_clamp(28/20, 5, 10, 60)
        {"name": "valid_double_support_pct",
         "constraint": "double_support_pct >= 10.0 AND double_support_pct <= 60.0",
         "tag": "gait"},
    ]
