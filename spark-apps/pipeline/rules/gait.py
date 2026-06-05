def get_gait_rules() -> list[dict]:
    """
    Reglas de calidad para la Fuente C (eventos de zancada).
    Rangos derivados de _STRIDE_DUR_BASE, _STRIDE_LEN_BASE, _SWING_PROP,
    _FOOT_CLEAR_BASE, _TOE_OFF_BASE, _HEEL_STRIKE_BASE y _LATERAL_BASE.

    Se toman los límites más amplios entre todos los estados para que ningún
    dato válido sea rechazado. Los ángulos admiten valores negativos:
    toe_off llega a -25° (robusto), heel_strike baja hasta -10° (frágil).
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

        # Marcha — límites más amplios entre los tres estados
        # _STRIDE_DUR_BASE: min lo=0.85, max hi=2.50
        {"name": "valid_stride_duration",
         "constraint": "stride_duration_s >= 0.85 AND stride_duration_s <= 2.50",
         "tag": "gait"},
        # _STRIDE_LEN_BASE: min lo=0.20, max hi=1.45
        {"name": "valid_stride_length",
         "constraint": "stride_length_m >= 0.20 AND stride_length_m <= 1.45",
         "tag": "gait"},
        # _SWING_PROP × dur: mín=0.85×0.32=0.272, máx=2.50×0.44=1.10
        {"name": "valid_swing_time",
         "constraint": "swing_time_s >= 0.27 AND swing_time_s <= 1.10",
         "tag": "gait"},
        # dur − swing: mín=0.85×(1−0.44)=0.476, máx=2.50×(1−0.24)=1.90
        {"name": "valid_stance_time",
         "constraint": "stance_time_s >= 0.48 AND stance_time_s <= 1.90",
         "tag": "gait"},
        # _FOOT_CLEAR_BASE: min lo=0.01, max hi=0.18
        {"name": "valid_foot_clearance",
         "constraint": "foot_clearance_m >= 0.01 AND foot_clearance_m <= 0.18",
         "tag": "gait"},
        # _TOE_OFF_BASE: min lo=-25, max hi=5
        {"name": "valid_toe_off_angle",
         "constraint": "toe_off_angle_deg >= -25.0 AND toe_off_angle_deg <= 5.0",
         "tag": "gait"},
        # _HEEL_STRIKE_BASE: min lo=-10, max hi=18
        {"name": "valid_heel_strike_angle",
         "constraint": "heel_strike_angle_deg >= -10.0 AND heel_strike_angle_deg <= 18.0",
         "tag": "gait"},
        # _LATERAL_BASE: min lo=0.01, max hi=0.20
        {"name": "valid_lateral_excursion",
         "constraint": "lateral_excursion_m >= 0.01 AND lateral_excursion_m <= 0.20",
         "tag": "gait"},

        # Consistencia: stance = round(dur − swing, 4) → diferencia por redondeo ≈ 0
        {"name": "stride_time_consistent",
         "constraint": "ABS(stance_time_s + swing_time_s - stride_duration_s) < 0.001",
         "tag": "gait"},
    ]
