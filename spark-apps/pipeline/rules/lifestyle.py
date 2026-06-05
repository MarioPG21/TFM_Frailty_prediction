def get_lifestyle_rules() -> list[dict]:
    """
    Reglas de calidad para la Fuente B2 (encuestas de hábitos y condiciones crónicas).
    Rangos derivados de _PA_VIG_PARAMS, _PA_MOD_PARAMS, _SED_PARAMS,
    _DEPR_P, _HTN_P, _DIA_P y _ART_P del generador.
    """
    return [
        # Identidad
        {"name": "valid_response_id_ls",
         "constraint": "response_id IS NOT NULL",
         "tag": "lifestyle"},
        {"name": "valid_patient_id_ls",
         "constraint": "patient_id IS NOT NULL",
         "tag": "lifestyle"},

        # Actividad física — _PA_VIG_PARAMS: lo=0, hi=7 (días/semana)
        {"name": "valid_activity_vigorous",
         "constraint": "physical_activity_vigorous >= 0 AND physical_activity_vigorous <= 7",
         "tag": "lifestyle"},
        # _PA_MOD_PARAMS: lo=0, hi=7 (días/semana)
        {"name": "valid_activity_moderate",
         "constraint": "physical_activity_moderate >= 0 AND physical_activity_moderate <= 7",
         "tag": "lifestyle"},
        # _SED_PARAMS: min lo=1, max hi=18 (horas/día)
        {"name": "valid_sedentary",
         "constraint": "sedentary_hours_day >= 1.0 AND sedentary_hours_day <= 18.0",
         "tag": "lifestyle"},

        # Condiciones crónicas — bernoulli(p) ∈ {0, 1}
        {"name": "valid_depression_flag",
         "constraint": "depression IN (0, 1)",
         "tag": "lifestyle"},
        {"name": "valid_hypertension_flag",
         "constraint": "hypertension IN (0, 1)",
         "tag": "lifestyle"},
        {"name": "valid_diabetes_flag",
         "constraint": "diabetes IN (0, 1)",
         "tag": "lifestyle"},
        {"name": "valid_arthritis_flag",
         "constraint": "arthritis IN (0, 1)",
         "tag": "lifestyle"},
        # htn + dia + art + extra(0-2): máximo = 1+1+1+2 = 5
        {"name": "valid_num_chronic",
         "constraint": "num_chronic_conditions >= 0 AND num_chronic_conditions <= 5",
         "tag": "lifestyle"},
    ]
