import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os


def format_score(number):
    return f"+{number}" if number >= 0 else str(number)


def interpolate(value, x1, x2, y1, y2):
    return y1 + (value - x1) * (y2 - y1) / (x2 - x1)


# Data Processing Functions
def return_prev_exam_dates(sample, study_id, bmd_trend_values):
    study_date = sample["date_time"].values[0]
    study_date_normalized = pd.to_datetime(study_date).normalize()

    subset = bmd_trend_values[(bmd_trend_values.study_id == study_id)]
    filtered_bmd_trend_values = subset[(subset["date"] < study_date_normalized)]

    if len(filtered_bmd_trend_values) == 0:
        return None, None
    else:
        ## previous
        reference_date_row = filtered_bmd_trend_values.loc[
            filtered_bmd_trend_values["date"].idxmax()
        ]
        reference_date_str = reference_date_row["date"]

        grouped = filtered_bmd_trend_values.groupby("date")["body_part"].apply(set)
        required_parts = set(filtered_bmd_trend_values["body_part"].unique())
        valid_dates = grouped[grouped.apply(lambda x: required_parts.issubset(x))]

        if len(valid_dates) == 0:
            baseline_date_str = None
        else:
            baseline_date_str = valid_dates.index.min()

        return reference_date_str, baseline_date_str


# Turn Findings into a string
def get_findings_text(
    findings_df, age, findings_type, reference=None, cannot_be_compared=False
):
    bmd = findings_df["bmd"].values[0]
    t_score = findings_df["t_score"].values[0]
    z_score = findings_df["z_score"].values[0]

    findings = []

    if age >= 50:
        findings.append(
            f"{findings_type} = {bmd} g/cm2. T-score = {format_score(t_score)}"
        )
    else:
        findings.append(
            f"{findings_type} = {bmd} g/cm2. Z-score = {format_score(z_score)}"
        )

    if reference is not None and not reference.empty and not cannot_be_compared:
        reference_bmd = reference["bmd"].values[0]
        change = bmd - reference_bmd
        change_type = ""
        if change > 0:
            change_type = "increased"
        else:
            change_type = "decreased"
        percent_change = change / reference_bmd * 100
        findings.append(
            f"This value has {change_type} by {abs(round(change, 3))} g/cm2 ({abs(round(percent_change, 1))}%) compared to the previous."
        )

    findings.append("\n")
    return " ".join(findings)


## Get Value based on Age
def get_numerical_values(findings_df, age):
    t_score = findings_df["t_score"].values[0]
    z_score = findings_df["z_score"].values[0]
    if age >= 50:
        return t_score
    else:
        return z_score


## Get bmd change value
def get_change_value(findings_df, reference_df):
    bmd = findings_df["bmd"].values[0]
    if reference_df is not None and not reference_df.empty:
        reference_bmd = reference_df["bmd"].values[0]
        change = bmd - reference_bmd
        change = round(change, 3)
        return change
    else:
        return None


## Get change based on hospital and lsc
def get_change_type(value, region, institution_name):
    if institution_name == "Mississauga Hospital":
        lsc_spine = 0.033
        lsc_total = 0.017
        if region == "lumbar spine":
            if abs(value) > lsc_spine:
                if value > 0:
                    return {
                        "region": "lumbar spine",
                        "change": "increase",
                        "significant": True,
                    }
                else:
                    return {
                        "region": "lumbar spine",
                        "change": "decrease",
                        "significant": True,
                    }
            else:
                return {"region": "lumbar spine", "significant": False, "change": None}
        elif region == "hip":
            if abs(value) > lsc_total:
                if value > 0:
                    return {"region": "hip", "change": "increase", "significant": True}
                else:
                    return {"region": "hip", "change": "decrease", "significant": True}
            else:
                return {"region": "hip", "significant": False, "change": None}

    elif institution_name == "Queensway Hospital":
        lsc_spine = 0.039
        lsc_total = 0.024
        if region == "lumbar spine":
            if abs(value) > lsc_spine:
                if value > 0:
                    return {
                        "region": "lumbar spine",
                        "change": "increase",
                        "significant": True,
                    }
                else:
                    return {
                        "region": "lumbar spine",
                        "change": "decrease",
                        "significant": True,
                    }
            else:
                return {"region": "lumbar spine", "significant": False, "change": None}
        elif region == "hip":
            if abs(value) > lsc_total:
                if value > 0:
                    return {"region": "hip", "change": "increase", "significant": True}
                else:
                    return {"region": "hip", "change": "decrease", "significant": True}
            else:
                return {"region": "hip", "significant": False, "change": None}

    elif institution_name == "Credit Valley Hospital":
        lsc_spine = 0.036
        lsc_total = 0.024

        if region == "lumbar spine":
            if abs(value) > lsc_spine:
                if value > 0:
                    return {
                        "region": "lumbar spine",
                        "change": "increase",
                        "significant": True,
                    }
                else:
                    return {
                        "region": "lumbar spine",
                        "change": "decrease",
                        "significant": True,
                    }
            else:
                return {"region": "lumbar spine", "significant": False, "change": None}
        elif region == "hip":
            if abs(value) > lsc_total:
                if value > 0:
                    return {"region": "hip", "change": "increase", "significant": True}
                else:
                    return {"region": "hip", "change": "decrease", "significant": True}
            else:
                return {"region": "hip", "significant": False, "change": None}


## Logic to exclude verterbrae
def filter_vertebra_by_tscore(t_scores):
    # Filter out None values from t_scores
    filtered_scores = {
        region: score for region, score in t_scores.items() if score is not None
    }

    # Sort t_scores by value in descending order
    sorted_scores = sorted(filtered_scores.items(), key=lambda x: x[1], reverse=True)

    # List to hold regions to exclude
    exclude_regions = []

    # Check L1
    if "L1" in filtered_scores:
        if (
            len(sorted_scores) > 1
            and sorted_scores[0][0] == "L1"
            and sorted_scores[0][1] - sorted_scores[1][1] > 1
        ):
            exclude_regions.append("L1")
        elif (
            len(sorted_scores) > 2
            and sorted_scores[1][0] == "L1"
            and sorted_scores[1][1] - sorted_scores[2][1] > 1
        ):
            exclude_regions.append("L1")

    # Check L4
    if "L4" in filtered_scores:
        if (
            len(sorted_scores) > 1
            and sorted_scores[-1][0] == "L4"
            and sorted_scores[-2][1] - sorted_scores[-1][1] > 1
        ):
            exclude_regions.append("L4")
        elif (
            len(sorted_scores) > 2
            and sorted_scores[-2][0] == "L4"
            and sorted_scores[-3][1] - sorted_scores[-2][1] > 1
        ):
            exclude_regions.append("L4")

    # Return a new dictionary excluding the regions
    return {
        region: score
        for region, score in t_scores.items()
        if region not in exclude_regions
    }


## Select the best vertebra combination based on exclusion
def select_vertebra_combination(filtered_t_scores):
    vertebrae = list(filtered_t_scores.keys())

    # Define the possible combinations in order of preference
    combinations = [["L1", "L2", "L3", "L4"], ["L1", "L2", "L3"], ["L2", "L3", "L4"]]

    # Find the first valid combination that is fully included in the available vertebrae
    for combo in combinations:
        if all(v in vertebrae for v in combo):
            return combo

    # If no valid combination is found, return an empty list
    return []


def get_study_bmd_values(study_id, bmd_values):
    study_bmd_values = bmd_values.loc[bmd_values.study_id == study_id]
    if study_bmd_values.empty:
        raise ValueError(f"No bmd_values for {study_id}")
    return study_bmd_values


def get_institution_name(study_id, studies):
    study = studies.loc[studies.id == study_id]
    return study["institution_name"].values[0]


def get_study_bmd_values_reference(study_id, reference_date_str, bmd_trend_values):
    return bmd_trend_values.loc[
        (bmd_trend_values.study_id == study_id)
        & (bmd_trend_values.date == reference_date_str)
    ]


def check_vertebra_combination(lumbar_spine, age):
    exclude_l4 = False
    selected_combination = None

    if age >= 50:
        l1 = lumbar_spine.loc[lumbar_spine.region == "L1"]
        l2 = lumbar_spine.loc[lumbar_spine.region == "L2"]
        l3 = lumbar_spine.loc[lumbar_spine.region == "L3"]
        l4 = lumbar_spine.loc[lumbar_spine.region == "L4"]

        t_scores = {
            "L1": l1["t_score"].values[0] if not l1.empty else None,
            "L2": l2["t_score"].values[0] if not l2.empty else None,
            "L3": l3["t_score"].values[0] if not l3.empty else None,
            "L4": l4["t_score"].values[0] if not l4.empty else None,
        }

        filtered_t_scores = filter_vertebra_by_tscore(t_scores)

        # Select the best vertebra combination
        selected_combination = select_vertebra_combination(filtered_t_scores)

        if selected_combination != ["L1", "L2", "L3", "L4"]:
            exclude_l4 = True

    return exclude_l4, selected_combination


def handle_lumbar_spine_combination(
    lumbar_spine,
    selected_combination,
    report_reference,
    age,
    institution_name,
    findings,
    change_values,
    scores,
    cannot_be_compared,
):
    region_mapping = {
        "L1-L4": "Trend L1-L4",
        "L1-L3": "Trend L1-L3",
        "L2-L4": "Trend L2-L4",
    }
    combination_text_mapping = {
        "L1-L4": "LUMBAR SPINE (L1-L4)",
        "L1-L3": "LUMBAR SPINE (L1-L3)",
        "L2-L4": "LUMBAR SPINE (L2-L4)",
    }

    region = "-".join(selected_combination)
    lumbar_region = lumbar_spine.loc[lumbar_spine.region == region]
    lumbar_region_reference = report_reference.loc[
        report_reference.region == region_mapping[region]
    ]

    if not lumbar_region.empty:
        findings.append(
            get_findings_text(
                lumbar_region,
                age,
                combination_text_mapping[region],
                lumbar_region_reference,
                cannot_be_compared,
            )
        )
        if selected_combination in [["L1", "L2", "L3"], ["L2", "L3", "L4"]]:
            excluded_vertebra = (
                "L4" if selected_combination == ["L1", "L2", "L3"] else "L1"
            )
            findings.append(
                f"{excluded_vertebra} has been excluded from these calculations because it is significantly different than all the other vertebral bodies.\n"
            )

        value = get_change_value(lumbar_region, lumbar_region_reference)
        if value is not None and cannot_be_compared != False:
            change_values.append(
                get_change_type(value, "lumbar spine", institution_name)
            )
        scores.append(get_numerical_values(lumbar_region, age))

    return findings


def handle_lumbar_spine(
    lumbar_spine,
    study_bmd_values_reference,
    age,
    institution_name,
    findings,
    change_values,
    scores,
    cannot_be_compared,
):
    lumbar_scores = 0
    exclude_l4 = False
    if not lumbar_spine.empty:
        exclude_l4, selected_combination = check_vertebra_combination(lumbar_spine, age)
        if exclude_l4 == False:
            l1_l4 = lumbar_spine.loc[lumbar_spine.region == "L1-L4"]
            l1_l4_reference = study_bmd_values_reference.loc[
                study_bmd_values_reference.region == "Trend L1-L4"
            ]
            if l1_l4 is not None and not l1_l4.empty:
                findings.append(
                    get_findings_text(
                        l1_l4,
                        age,
                        "LUMBAR SPINE (L1-L4)",
                        l1_l4_reference,
                        cannot_be_compared,
                    )
                )
                value = get_change_value(l1_l4, l1_l4_reference)
                if value != None and cannot_be_compared != False:
                    change_values.append(
                        get_change_type(value, "lumbar spine", institution_name)
                    )
                scores.append(get_numerical_values(l1_l4, age))
                lumbar_scores = get_numerical_values(l1_l4, age)
        elif selected_combination == ["L1", "L2", "L3"]:
            l1_l3 = lumbar_spine.loc[lumbar_spine.region == "L1-L3"]
            l1_l3_reference = study_bmd_values_reference.loc[
                study_bmd_values_reference.region == "Trend L1-L3"
            ]
            if not l1_l3.empty:
                findings.append(
                    get_findings_text(
                        l1_l3,
                        age,
                        "LUMBAR SPINE (L1-L3)",
                        l1_l3_reference,
                        cannot_be_compared,
                    )
                )
                findings.append(
                    "L4 has been excluded from these calculations because it is significantly different than all the other vertebral bodies.\n"
                )
                value = get_change_value(l1_l3, l1_l3_reference)
                if value != None and cannot_be_compared != False:
                    change_values.append(
                        get_change_type(value, "lumbar spine", institution_name)
                    )
                scores.append(get_numerical_values(l1_l3, age))
                lumbar_scores = get_numerical_values(l1_l3, age)
        elif selected_combination == ["L2", "L3", "L4"]:
            l2_l4 = lumbar_spine.loc[lumbar_spine.region == "L2-L4"]
            l2_l4_reference = study_bmd_values_reference.loc[
                study_bmd_values_reference.region == "Trend L2-L4"
            ]
            if not l2_l4.empty:
                findings.append(
                    get_findings_text(
                        l2_l4,
                        age,
                        "LUMBAR SPINE (L2-L4)",
                        l2_l4_reference,
                        cannot_be_compared,
                    )
                )
                findings.append(
                    "L1 has been excluded from these calculations because it is significantly different than all the other vertebral bodies.\n"
                )
                value = get_change_value(l2_l4, l2_l4_reference)
                if value != None and cannot_be_compared != False:
                    change_values.append(
                        get_change_type(value, "lumbar spine", institution_name)
                    )
                scores.append(get_numerical_values(l2_l4, age))
                lumbar_scores = get_numerical_values(l2_l4, age)
        else:
            findings.append(
                "LUMBAR SPINE: L1 and L4 have both been excluded from these calculations has been excluded from these calculations because it is significantly different than all the other vertebral bodies. No valid scores to report.\n"
            )
    else:
        findings.append("Lumbar spine: No valid scans available.")
    return findings, scores, lumbar_scores, exclude_l4


def handle_femur(
    study_bmd_values,
    side,
    sex,
    age,
    study_bmd_values_reference,
    cannot_be_compared,
    scores,
    change_values,
    institution_name,
):
    findings = []
    femur = study_bmd_values.loc[study_bmd_values.body_part == f"{side} Femur"]

    if not femur.empty:
        neck = femur.loc[femur.region == "Neck"]
        neck_reference = study_bmd_values.loc[study_bmd_values.region == f"Trend Neck"]

        if not neck.empty:
            if sex == "male":
                score_column = "t_score" if age >= 50 else "z_score"
                min_neck = neck.loc[neck[score_column] == neck[score_column].min()]
                max_neck = neck.loc[neck[score_column] == neck[score_column].max()]
                findings.append(
                    get_findings_text(
                        min_neck,
                        age,
                        f"{side.upper()} FEMORAL NECK",
                        neck_reference,
                        cannot_be_compared,
                    )
                )
                scores.append(get_numerical_values(min_neck, age))
                findings.append(
                    get_findings_text(
                        max_neck, age, f"{side.upper()} FEMORAL NECK (FEMALE REFERENCE)"
                    )
                )
            else:
                findings.append(
                    get_findings_text(
                        neck,
                        age,
                        "LEFT FEMORAL NECK",
                        neck_reference,
                        cannot_be_compared,
                    )
                )
                scores.append(get_numerical_values(neck, age))

        total = femur.loc[femur.region == "Total"]
        total_reference = study_bmd_values_reference.loc[
            study_bmd_values_reference.region == f"Trend Total"
        ]
        if not total.empty:
            if sex == "male":
                score_column = "t_score" if age >= 50 else "z_score"
                min_total = total.loc[total[score_column] == total[score_column].min()]
                findings.append(
                    get_findings_text(
                        min_total,
                        age,
                        f"TOTAL PROXIMAL {side.upper()} FEMUR",
                        total_reference,
                        cannot_be_compared,
                    )
                )
                value = get_change_value(total, total_reference)
                if value and not cannot_be_compared:
                    change_values.append(
                        get_change_type(value, "hip", institution_name)
                    )
                scores.append(get_numerical_values(min_total, age))
            else:
                findings.append(
                    get_findings_text(
                        total,
                        age,
                        "TOTAL PROXIMAL LEFT FEMUR",
                        total_reference,
                        cannot_be_compared,
                    )
                )
                value = get_change_value(total, total_reference)
                if value and not cannot_be_compared:
                    change_values.append(
                        get_change_type(value, "hip", institution_name)
                    )
                scores.append(get_numerical_values(total, age))
    return findings


def handle_forearm(study_bmd_values, side, age, study_bmd_values_reference, scores):
    findings = []
    forearm = study_bmd_values.loc[study_bmd_values.body_part == f"{side} Forearm"]
    if not forearm.empty:
        radius_1_3 = forearm.loc[forearm.region == "Radius 33%"]
        radius_1_3_reference = study_bmd_values_reference.loc[
            study_bmd_values_reference.region == f"Trend Radius 33%"
        ]
        if not radius_1_3.empty:
            findings.append(
                get_findings_text(
                    radius_1_3, age, f"1/3 {side.upper()} RADIUS", radius_1_3_reference
                )
            )
            scores.append(get_numerical_values(radius_1_3, age))

    return findings


def get_fracture_risk_from_femur(
    femur,
    sex,
    age,
    lumbar_scores,
    fragility_fracture_history,
    glucocorticoid_history,
    fragility_hip_fracture,
    fragility_vertebral_fracture,
    two_or_more_fragility_fractures,
):
    if not femur.empty:
        neck = femur.loc[femur.region == "Neck"]
        if not neck.empty:
            if sex == "male":
                female_neck = neck.loc[neck["t_score"] == neck["t_score"].max()]
            else:
                female_neck = neck

            if not female_neck.empty:
                t_score = female_neck["t_score"].values[0]
                return get_fracture_risk(
                    t_score,
                    sex,
                    age,
                    lumbar_scores,
                    fragility_fracture_history,
                    glucocorticoid_history,
                    fragility_hip_fracture,
                    fragility_vertebral_fracture,
                    two_or_more_fragility_fractures,
                )
    return "Cannot be calculated"


def get_fracture_risk_from_lumbar(lumbar_spine, age):
    if not lumbar_spine.empty:
        l1_l4 = lumbar_spine.loc[lumbar_spine.region == "L1-L4"]
        if not l1_l4.empty:
            if age >= 50:
                t_score = l1_l4["t_score"].values[0]
                if t_score <= -2.5:
                    return "Moderate, 10-20%"
    return "Cannot be calculated"


def calculate_fracture_risk(
    study_bmd_values,
    sex,
    age,
    lumbar_scores,
    fragility_fracture_history,
    glucocorticoid_history,
    fragility_hip_fracture,
    fragility_vertebral_fracture,
    two_or_more_fragility_fractures,
    exclude_l4,
):
    left_femur = study_bmd_values.loc[study_bmd_values.body_part == "Left Femur"]
    right_femur = study_bmd_values.loc[study_bmd_values.body_part == "Right Femur"]
    lumbar_spine = study_bmd_values.loc[study_bmd_values.body_part == "AP Spine"]

    fracture_risk = get_fracture_risk_from_femur(
        left_femur,
        sex,
        age,
        lumbar_scores,
        fragility_fracture_history,
        glucocorticoid_history,
        fragility_hip_fracture,
        fragility_vertebral_fracture,
        two_or_more_fragility_fractures,
    )

    if fracture_risk == "Cannot be calculated":
        fracture_risk = get_fracture_risk_from_femur(
            right_femur,
            sex,
            age,
            lumbar_scores,
            fragility_fracture_history,
            glucocorticoid_history,
            fragility_hip_fracture,
            fragility_vertebral_fracture,
            two_or_more_fragility_fractures,
        )

    if fracture_risk == "Cannot be calculated" and exclude_l4 == False:
        fracture_risk = get_fracture_risk_from_lumbar(lumbar_spine, age)

    return fracture_risk


def return_findings(
    study_id,
    sex,
    age,
    studies,
    bmd_values,
    bmd_trend_values,
    fragility_fracture_history,
    glucocorticoid_history,
    fragility_hip_fracture,
    fragility_vertebral_fracture,
    two_or_more_fragility_fractures,
    cannot_be_compared,
    institution_name,
):
    findings = []
    scores = []
    lumbar_scores = 0
    change_values = []

    study = studies.loc[studies.id == study_id]

    study_bmd_values = get_study_bmd_values(study_id, bmd_values)

    if study_bmd_values.empty:
        raise ValueError(f"No bmd_values for {study_id}")

    reference_date_str, baseline_date_str = return_prev_exam_dates(
        study, study_id, bmd_trend_values
    )

    study_bmd_values_reference = get_study_bmd_values_reference(
        study_id, reference_date_str, bmd_trend_values
    )

    lumbar_spine = study_bmd_values.loc[study_bmd_values.body_part == "AP Spine"]
    findings, scores, lumbar_scores, exclude_l4 = handle_lumbar_spine(
        lumbar_spine,
        study_bmd_values_reference,
        age,
        institution_name,
        findings,
        change_values,
        scores,
        cannot_be_compared,
    )

    findings.extend(
        handle_femur(
            study_bmd_values,
            "Left",
            sex,
            age,
            study_bmd_values_reference,
            cannot_be_compared,
            scores,
            change_values,
            institution_name,
        )
    )
    findings.extend(
        handle_femur(
            study_bmd_values,
            "Right",
            sex,
            age,
            study_bmd_values_reference,
            cannot_be_compared,
            scores,
            change_values,
            institution_name,
        )
    )
    findings.extend(
        handle_forearm(
            study_bmd_values, "Left", age, study_bmd_values_reference, scores
        )
    )
    findings.extend(
        handle_forearm(
            study_bmd_values, "Right", age, study_bmd_values_reference, scores
        )
    )

    if len(scores) == 0:
        raise ValueError(f"No t/z scores have been found study {study_id}")

    diagnostic_category = get_diagnostic_category(age, scores)
    findings.append(f"BONE MINERAL DENSITY: {diagnostic_category}\n")

    fracture_risk = calculate_fracture_risk(
        study_bmd_values,
        sex,
        age,
        lumbar_scores,
        fragility_fracture_history,
        glucocorticoid_history,
        fragility_hip_fracture,
        fragility_vertebral_fracture,
        two_or_more_fragility_fractures,
        exclude_l4,
    )

    if fracture_risk != "Cannot be calculated":
        findings.append(f"10 YEAR ABSOLUTE FRACTURE RISK: {fracture_risk}\n")

    return (
        "\n".join(findings),
        scores,
        diagnostic_category,
        fracture_risk,
        change_values,
    )


def get_fracture_risk(
    t_score,
    sex,
    age,
    lumbar_scores,
    fragility_fracture_history=False,
    glucocorticoid_history=False,
    fragility_hip_fracture=False,
    fragility_vertebral_fracture=False,
    two_or_more_fragility_fractures=False,
):
    if age < 50:
        return "Fracture risk cannot be stated in patients less than 50 years of age."

    if fragility_fracture_history == True & glucocorticoid_history == True:
        return "High, greater than 20%"
    elif fragility_hip_fracture == True:
        return "High, greater than 20%"
    elif fragility_vertebral_fracture == True:
        return "High, greater than 20%"
    elif two_or_more_fragility_fractures == True:
        return "High, greater than 20%"

    # Define the CAROC 2010 tables for women and men
    caroc_table_women = [
        (50, -2.5, -3.8),
        (55, -2.5, -3.8),
        (60, -2.3, -3.7),
        (65, -1.9, -3.5),
        (70, -1.7, -3.2),
        (75, -1.2, -2.9),
        (80, -0.5, -2.6),
        (85, 0.1, -2.2),
    ]

    caroc_table_men = [
        (50, -2.5, -3.9),
        (55, -2.5, -3.9),
        (60, -2.5, -3.7),
        (65, -2.4, -3.7),
        (70, -2.3, -3.7),
        (75, -2.3, -3.8),
        (80, -2.1, -3.8),
        (85, -2.0, -3.8),
    ]

    # Select the appropriate table based on sex
    caroc_table = caroc_table_women if sex.lower() == "female" else caroc_table_men

    # Find the two closest age groups for interpolation
    if age >= 85:
        age_group1, age_group2 = caroc_table[-2], caroc_table[-1]
    else:
        for i in range(len(caroc_table) - 1):
            if caroc_table[i][0] <= age < caroc_table[i + 1][0]:
                age_group1, age_group2 = caroc_table[i], caroc_table[i + 1]
                break

    age1, low1, high1 = age_group1
    age2, low2, high2 = age_group2
    low_risk_threshold = interpolate(age, age1, age2, low1, low2)
    high_risk_threshold = interpolate(age, age1, age2, high1, high2)

    # Determine the risk category
    if t_score > low_risk_threshold:
        if fragility_fracture_history == True | glucocorticoid_history == True:
            risk_category = "Moderate, 10-20%"
        elif lumbar_scores <= -2.5:
            risk_category = "Moderate, 10-20%"
        else:
            risk_category = "Low, less than 10%"
    elif low_risk_threshold >= t_score > high_risk_threshold:
        if fragility_fracture_history == True | glucocorticoid_history == True:
            risk_category = "High, greater than 20%"
        else:
            risk_category = "Moderate, 10-20%"
    else:
        risk_category = "High, greater than 20%"

    return risk_category


def strip_risk(risk_category):
    if "Low" in risk_category:
        return "Low"
    elif "Moderate" in risk_category:
        return "Moderate"
    elif "High" in risk_category:
        return "High"
    elif "Within" in risk_category:
        return "Within"
    elif "Below" in risk_category:
        return "Below"
    else:
        return risk_category


def get_diagnostic_category(age, scores):
    diagnostic_category = ""
    if age >= 50:
        if min(scores) <= -2.5:
            diagnostic_category = "Osteoporosis"
        elif min(scores) > -2.5 and min(scores) < -1:
            diagnostic_category = "Low bone mass"
        elif min(scores) >= -1:
            diagnostic_category = "Normal bone mass"
    else:
        if min(scores) <= -2.0:
            diagnostic_category = "Below expected range for age"
        else:
            diagnostic_category = "Within expected range for age"
    return diagnostic_category


def get_change_statement(change_values):
    change_statement = []
    negative_change = False
    for change_value in change_values:
        region = change_value["region"]
        change = change_value["change"]
        significant = change_value["significant"]
        if significant:
            change_statement.append(
                f"There has been a statistically SIGNIFICANT {change.upper()} in BMD in the {region}."
            )
            if change == "decrease":
                negative_change = True
        else:
            change_statement.append(
                f"There has been NO statistically significant change in BMD in the {region} from the prior examination."
            )

    if negative_change:
        change_statement.append("Current management could be reassessed.")
    else:
        change_statement.append("Current management remains appropriate.")
    return " ".join(change_statement)


def get_lsc(institution_name):
    if institution_name == "Mississauga Hospital":
        return "LSC (least significant change) at MH:\n\
Lumbar spine - 0.033 gm/cm2\n\
Total femur - 0.017 gm/cm2"
    elif institution_name == "Queensway Hospital":
        return "LSC (least significant change) at QH:\n\
Lumbar spine - 0.039 gm/cm2\n\
Total femur - 0.024 gm/cm2"
    elif institution_name == "Credit Valley Hospital":
        return "LSC (least significant change) at CVH:\n\
Lumbar spine - 0.036 gm/cm2\n\
Total femur - 0.024gm/cm2"


def get_fracture_risk_modifer_statement(
    fragility_fracture_history=False, glucocorticoid_history=False
):
    if fragility_fracture_history == True and glucocorticoid_history == True:
        return "Fracture risk has been modified by the history of prior fragility fracture and glucocorticoid history."
    elif fragility_fracture_history == True:
        return "Fracture risk has been modified by the history of prior fragility fracture."
    elif glucocorticoid_history == True:
        return "Fracture risk has been modified by glucocorticoid history."


def get_cannot_be_compared_statement():
    return (
        "Direct comparison to the prior examination cannot be performed as it was performed on a different machine and any comparison would not be statistically valid.\n\n"
        "This examination can serve as a baseline for future follow-up examinations, however."
    )


def append_common_statements(
    findings,
    change_statement,
    fracture_risk_modifier_statement,
    cannot_be_compared,
    lsc,
):
    if change_statement:
        findings.append(change_statement)
    if fracture_risk_modifier_statement:
        findings.append(fracture_risk_modifier_statement)
    if cannot_be_compared:
        findings.append(get_cannot_be_compared_statement())
    if lsc:
        findings.append(lsc)


def get_follow_up_statement(risk):
    if risk == "LOW":
        return "Follow-up suggested in 3 years."
    elif risk == "MODERATE":
        return "Follow-up suggested in 1-3 years, and bone active medication could be considered."
    elif risk == "HIGH":
        return "Follow-up suggested in 1 year, and bone active medication could be considered."
    elif risk in {"WITHIN", "BELOW"}:
        return (
            "Follow-up suggested in 3 years."
            if risk == "WITHIN"
            else "Follow-up suggested in 1 year."
        )
    return None


def return_summary(
    diagnostic_category,
    fracture_risk,
    institution_name,
    change_values,
    fragility_fracture_history,
    glucocorticoid_history,
    cannot_be_compared,
):
    findings = []
    risk = strip_risk(fracture_risk).upper()
    diagnostic = diagnostic_category.upper()

    change_statement = (
        get_change_statement(change_values)
        if change_values and not cannot_be_compared
        else None
    )
    lsc = get_lsc(institution_name) if change_statement else None
    fracture_risk_modifier_statement = (
        get_fracture_risk_modifer_statement(
            fragility_fracture_history, glucocorticoid_history
        )
        if fragility_fracture_history or glucocorticoid_history
        else None
    )

    findings.append(f"This patient has {diagnostic} with {risk} FRACTURE RISK.")

    if risk == "MODERATE":
        findings.append(
            "In patients with moderate fracture risk, it would be appropriate to consider a lateral x-ray of the thoracic and lumbar spine from T4-L4 to assess for possible compression fractures."
        )
        findings.append(
            "The presence of a compression fracture of more than 25% would place the patient into the HIGH RISK category for future fractures and could change management strategies."
        )

    append_common_statements(
        findings,
        change_statement,
        fracture_risk_modifier_statement,
        cannot_be_compared,
        lsc,
    )

    follow_up_statement = get_follow_up_statement(risk)
    if follow_up_statement:
        findings.append(follow_up_statement)

    if risk in {"WITHIN", "BELOW"}:
        findings.append(
            "Fracture risk cannot be stated in patients less than 50 years of age."
        )

    return "\n\n".join(findings)


def get_report_text(reference_examination, technique, findings, summary):
    return f"""
EXAM: 
[<Examination Description>] 

CLINICAL INDICATION: 
[<Reason For Exam>] 

REFERENCE EXAMINATIONS: 
{reference_examination}

TECHNIQUE: 
{technique}

FINDINGS:
{findings} 

SUMMARY: 
{summary}

CAROC recommendations (2010) age 50 years and older: 
T-score between -1 and -2.5 = low bone mass 
T-score -2.5 or less = osteoporosis 

Fragility fractures of spine or hip or 2 fragility fractures elsewhere = osteoporosis and high fracture risk regardless of T-score 

Bisphosphonate therapy may lower fracture risk. 
"""


def get_database_connection():
    database_uri = os.getenv("DATABASE_URI").replace(
        "postgresql", "postgresql+psycopg2"
    )
    return create_engine(database_uri)


def load_dataframes(conn):
    patients = pd.read_sql("SELECT * FROM patients", conn)
    studies = pd.read_sql("SELECT * FROM studies", conn)
    bmd_values = pd.read_sql("SELECT * FROM bmd_values", conn)
    bmd_trend_values = pd.read_sql("SELECT * FROM bmd_trend_values", conn)
    bmd_trend_values["date"] = pd.to_datetime(bmd_trend_values["date"])
    return patients, studies, bmd_values, bmd_trend_values


def get_study_details(studies, accession):
    study = studies.loc[studies.accession == accession]
    study_details = {
        "study_id": study["id"].values[0],
        "patient_id": study["patient_id"].values[0],
        "age": study["age"].values[0],
        "description": study["description"].values[0],
        "date_time": study["date_time"].values[0],
        "institution_name": study["institution_name"].values[0],
    }
    return study_details


def get_patient_sex(patients, patient_id):
    sex = patients.loc[patients.id == patient_id]["sex"].values[0]
    return sex.replace("F", "female").replace("M", "male")


def get_previous_exam_dates(study_id, bmd_trend_values):
    prev_exam_dates = bmd_trend_values[bmd_trend_values.study_id == study_id][
        "date"
    ].unique()
    return prev_exam_dates


def format_exam_dates(reference_date_str, baseline_date_str):
    prev_date = (
        pd.to_datetime(reference_date_str).strftime("%B %d, %Y")
        if reference_date_str
        else None
    )
    baseline_date = (
        pd.to_datetime(baseline_date_str).strftime("%Y") if baseline_date_str else None
    )
    return prev_date, baseline_date


def create_reference_examination(prev_exam_dates, age, sex, prev_date, baseline_date):
    if len(prev_exam_dates) > 2:
        reference_examination = f"Multiple previous examinations, including a baseline on {baseline_date} and the most recent on {prev_date}."
    elif len(prev_exam_dates) == 2:
        reference_examination = (
            f"Baseline on {baseline_date} and the previous on {prev_date}."
        )
    elif len(prev_exam_dates) == 1:
        reference_examination = f"Previous examination on {prev_date}."
    else:
        reference_examination = "None."

    technique = f"A {'repeat' if prev_exam_dates else 'baseline'} bone density study was obtained on this {age} year old {sex}"

    return technique, reference_examination


def add_cannot_be_compared_notice(reference_examination):
    return (
        reference_examination
        + "\n\nThe BMD machine has been replaced at this hospital site since the prior examination and therefore direct statistical comparison is not possible.\n\nThis current study will serve as a baseline for future examinations, however."
    )


def process_sample(
    accession,
    fragility_fracture_history=False,
    glucocorticoid_history=False,
    fragility_hip_fracture=False,
    fragility_vertebral_fracture=False,
    two_or_more_fragility_fractures=False,
    cannot_be_compared=False,
):
    conn = get_database_connection()
    patients, studies, bmd_values, bmd_trend_values = load_dataframes(conn)

    study_details = get_study_details(studies, accession)
    patient_sex = get_patient_sex(patients, study_details["patient_id"])

    prev_exam_dates = get_previous_exam_dates(
        study_details["study_id"], bmd_trend_values
    )
    reference_date_str, baseline_date_str = return_prev_exam_dates(
        studies, study_details["study_id"], bmd_trend_values
    )
    prev_date, baseline_date = format_exam_dates(reference_date_str, baseline_date_str)

    technique, reference_examination = create_reference_examination(
        prev_exam_dates, study_details["age"], patient_sex, prev_date, baseline_date
    )

    if cannot_be_compared:
        reference_examination = add_cannot_be_compared_notice(reference_examination)

    findings, scores, diagnostic_category, fracture_risk, change_values = (
        return_findings(
            study_details["study_id"],
            patient_sex,
            study_details["age"],
            studies,
            bmd_values,
            bmd_trend_values,
            fragility_fracture_history,
            glucocorticoid_history,
            fragility_hip_fracture,
            fragility_vertebral_fracture,
            two_or_more_fragility_fractures,
            cannot_be_compared,
            study_details["institution_name"],
        )
    )

    summary = return_summary(
        diagnostic_category,
        fracture_risk,
        study_details["institution_name"],
        change_values,
        fragility_fracture_history,
        glucocorticoid_history,
        cannot_be_compared,
    )

    return (
        reference_examination,
        technique,
        findings,
        summary,
        diagnostic_category,
        fracture_risk,
    )
