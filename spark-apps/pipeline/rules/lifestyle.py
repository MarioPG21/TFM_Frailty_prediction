def get_lifestyle_rules() -> list[dict]:
    """
    Reglas de calidad para la Fuente B2 (encuestas de estilo de vida).
    Rangos derivados de los parámetros del generador.
    """
    return [
        # Identidad
        {"name": "valid_response_id_ls",
         "constraint": "response_id IS NOT NULL",
         "tag": "lifestyle"},
        {"name": "valid_patient_id_ls",
         "constraint": "patient_id IS NOT NULL",
         "tag": "lifestyle"},

        # gauss_clamp(3000/6500, 2000, 0, 20000)
        {"name": "valid_steps_per_day",
         "constraint": "steps_per_day >= 0 AND steps_per_day <= 20000",
         "tag": "lifestyle"},
        # gauss_clamp(60/200, 80, 0, 600)
        {"name": "valid_moderate_exercise",
         "constraint": "moderate_exercise_min_week >= 0 AND moderate_exercise_min_week <= 600",
         "tag": "lifestyle"},
        # gauss_clamp(0.7/1.0, 0.2, 0.3, 2.0)
        {"name": "valid_protein_intake",
         "constraint": "protein_intake_g_per_kg >= 0.3 AND protein_intake_g_per_kg <= 2.0",
         "tag": "lifestyle"},
        # gauss_clamp(2/5, 2, 0, 10)
        {"name": "valid_social_contacts",
         "constraint": "social_contacts_per_week >= 0 AND social_contacts_per_week <= 10",
         "tag": "lifestyle"},
        # weighted_choice([0, 1, 2])
        {"name": "valid_tobacco_use",
         "constraint": "tobacco_use IN (0, 1, 2)",
         "tag": "lifestyle"},
        # weighted_choice([0, 1, 2, 3])
        {"name": "valid_alcohol_units",
         "constraint": "alcohol_units_per_week IN (0, 1, 2, 3)",
         "tag": "lifestyle"},
    ]
