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


def return_findings(
    study_id,
    sex,
    age,
    studies,
    bmd_values,
    bmd_trend_values,
):
    findings = []
    scores = []
    lumbar_scores = 0
    change_values = []

    study = studies.loc[studies.id == study_id]

    institution_name = study["institution_name"].values[0]

    report = bmd_values.loc[bmd_values.study_id == study_id]

    if report.empty:
        raise ValueError(f"No bmd_values for {study_id}")

    reference_date_str, baseline_date_str = return_prev_exam_dates(
        study, study_id, bmd_trend_values
    )

    report_reference = bmd_trend_values.loc[
        (bmd_trend_values.study_id == study_id)
        & (bmd_trend_values.date == reference_date_str)
    ]

    lumbar_spine = report.loc[report.body_part == "AP Spine"]
    l1_l4 = None

    if not lumbar_spine.empty:

        exclude_l4 = False

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

        if not exclude_l4:
            l1_l4 = lumbar_spine.loc[lumbar_spine.region == "L1-L4"]
            l1_l4_reference = report_reference.loc[
                report_reference.region == "Trend L1-L4"
            ]
            if l1_l4 is not None and not l1_l4.empty:
                findings.append(
                    get_findings_text(
                        l1_l4,
                        age,
                        "LUMBAR SPINE (L1-L4)",
                        l1_l4_reference,
                    )
                )
                value = get_change_value(l1_l4, l1_l4_reference)
                if value != None:
                    change_values.append(
                        get_change_type(value, "lumbar spine", institution_name)
                    )
                scores.append(get_numerical_values(l1_l4, age))
                lumbar_scores = get_numerical_values(l1_l4, age)

        elif selected_combination == ["L1", "L2", "L3"]:
            l1_l3 = lumbar_spine.loc[lumbar_spine.region == "L1-L3"]
            l1_l3_reference = report_reference.loc[
                report_reference.region == "Trend L1-L3"
            ]
            if not l1_l3.empty:
                findings.append(
                    get_findings_text(
                        l1_l3,
                        age,
                        "LUMBAR SPINE (L1-L3)",
                        l1_l3_reference,
                    )
                )
                findings.append(
                    "L4 has been excluded from these calculations because it is significantly different than all the other vertebral bodies.\n"
                )
                value = get_change_value(l1_l3, l1_l3_reference)
                if value != None:
                    change_values.append(
                        get_change_type(value, "lumbar spine", institution_name)
                    )
                scores.append(get_numerical_values(l1_l3, age))
                lumbar_scores = get_numerical_values(l1_l3, age)

        elif selected_combination == ["L2", "L3", "L4"]:
            l2_l4 = lumbar_spine.loc[lumbar_spine.region == "L2-L4"]
            l2_l4_reference = report_reference.loc[
                report_reference.region == "Trend L2-L4"
            ]
            if not l2_l4.empty:
                findings.append(
                    get_findings_text(
                        l2_l4,
                        age,
                        "LUMBAR SPINE (L2-L4)",
                        l2_l4_reference,
                    )
                )
                findings.append(
                    "L1 has been excluded from these calculations because it is significantly different than all the other vertebral bodies.\n"
                )
                value = get_change_value(l2_l4, l2_l4_reference)
                if value != None:
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

    left_femur = report.loc[report.body_part == "Left Femur"]
    if not left_femur.empty:
        neck = left_femur.loc[left_femur.region == "Neck"]
        neck_reference = report_reference.loc[report_reference.region == "Trend Neck"]
        if not neck.empty:
            if sex == "male":
                if age >= 50:
                    min_neck = neck.loc[neck["t_score"] == neck["t_score"].min()]
                else:
                    min_neck = neck.loc[neck["z_score"] == neck["z_score"].min()]
                findings.append(
                    get_findings_text(
                        min_neck,
                        age,
                        "LEFT FEMORAL NECK",
                        neck_reference,
                    )
                )
                scores.append(get_numerical_values(min_neck, age))

                if age >= 50:
                    max_neck = neck.loc[neck["t_score"] == neck["t_score"].max()]
                else:
                    max_neck = neck.loc[neck["z_score"] == neck["z_score"].max()]
                findings.append(
                    get_findings_text(
                        max_neck, age, "LEFT FEMORAL NECK (FEMALE REFERENCE)"
                    )
                )

            else:
                findings.append(
                    get_findings_text(
                        neck,
                        age,
                        "LEFT FEMORAL NECK",
                        neck_reference,
                    )
                )
                scores.append(get_numerical_values(neck, age))

        total = left_femur.loc[left_femur.region == "Total"]
        total_reference = report_reference.loc[report_reference.region == "Trend Total"]
        if not total.empty:
            if sex == "male":
                if age >= 50:
                    min_total = total.loc[total["t_score"] == total["t_score"].min()]
                else:
                    min_total = total.loc[total["z_score"] == total["z_score"].min()]
                findings.append(
                    get_findings_text(
                        min_total,
                        age,
                        "TOTAL PROXIMAL LEFT FEMUR",
                        total_reference,
                    )
                )
                value = get_change_value(total, total_reference)
                if value:
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
                    )
                )
                value = get_change_value(total, total_reference)
                if value:
                    change_values.append(
                        get_change_type(value, "hip", institution_name)
                    )
                scores.append(get_numerical_values(total, age))

    left_forearm = report.loc[report.body_part == "Left Forearm"]
    if not left_forearm.empty:
        radius_1_3 = left_forearm.loc[left_forearm.region == "Radius 33%"]
        radius_1_3_reference = report_reference.loc[
            report_reference.region == "Trend Radius 33%"
        ]
        if not radius_1_3.empty:
            findings.append(
                get_findings_text(
                    radius_1_3, age, "1/3 LEFT RADIUS", radius_1_3_reference
                )
            )
            scores.append(get_numerical_values(radius_1_3, age))

    right_femur = report.loc[report.body_part == "Right Femur"]
    if not right_femur.empty:
        neck = right_femur.loc[right_femur.region == "Neck"]
        if not neck.empty:
            if sex == "male":
                if age >= 50:
                    min_neck = neck.loc[neck["t_score"] == neck["t_score"].min()]
                else:
                    min_neck = neck.loc[neck["z_score"] == neck["z_score"].min()]
                findings.append(get_findings_text(min_neck, age, "RIGHT FEMORAL NECK"))
                scores.append(get_numerical_values(min_neck, age))

                if age >= 50:
                    max_neck = neck.loc[neck["t_score"] == neck["t_score"].max()]
                else:
                    max_neck = neck.loc[neck["z_score"] == neck["z_score"].max()]
                findings.append(
                    get_findings_text(
                        max_neck, age, "RIGHT FEMORAL NECK (FEMALE REFERENCE)"
                    )
                )

            else:
                findings.append(get_findings_text(neck, age, "RIGHT FEMORAL NECK"))
                scores.append(get_numerical_values(neck, age))

        total = right_femur.loc[right_femur.region == "Total"]
        if not total.empty:
            if sex == "male":
                if age >= 50:
                    min_total = total.loc[total["t_score"] == total["t_score"].min()]
                else:
                    min_total = total.loc[total["z_score"] == total["z_score"].min()]
                findings.append(
                    get_findings_text(min_total, age, "TOTAL PROXIMAL RIGHT FEMUR")
                )
                scores.append(get_numerical_values(min_total, age))
            else:
                findings.append(
                    get_findings_text(total, age, "TOTAL PROXIMAL RIGHT FEMUR")
                )
                scores.append(get_numerical_values(total, age))

    left_forearm = report.loc[report.body_part == "Right Forearm"]
    if not left_forearm.empty:
        radius_1_3 = left_forearm.loc[left_forearm.region == "Radius 33%"]
        if not radius_1_3.empty:
            findings.append(get_findings_text(radius_1_3, age, "1/3 RIGHT RADIUS"))
            scores.append(get_numerical_values(radius_1_3, age))

    if len(scores) == 0:
        raise ValueError(f"No t/z scires have been found study {study_id}")

    diagnostic_category = get_diagnostic_category(age, scores)
    findings.append(f"BONE MINERAL DENSITY: {diagnostic_category}\n")

    return (
        "\n".join(findings),
        diagnostic_category,
    )


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


def process_sample(
    accession,
):
    DATABASE_URI = os.getenv("DATABASE_URI")
    DATABASE_URI.replace("postgresql", "postgresql+psycopg2")
    conn = create_engine(DATABASE_URI)
    patients = pd.read_sql("SELECT * FROM patients", conn)
    studies = pd.read_sql("SELECT * FROM studies", conn)
    bmd_values = pd.read_sql("SELECT * FROM bmd_values", conn)
    bmd_trend_values = pd.read_sql("SELECT * FROM bmd_trend_values", conn)
    bmd_trend_values["date"] = pd.to_datetime(bmd_trend_values["date"])

    study = studies.loc[studies.accession == accession]
    study_id = study["id"].values[0]
    patient_id = study["patient_id"].values[0]
    sex = (
        patients.loc[patients.id == patient_id]["sex"]
        .values[0]
        .replace("F", "female")
        .replace("M", "male")
    )
    age = study["age"].values[0]
    findings, diagnostic_category = (
        return_findings(
            study_id,
            sex,
            age,
            studies,
            bmd_values,
            bmd_trend_values,
        )
    )
    return findings, diagnostic_category
