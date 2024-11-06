import pydicom


def convert_to_dict(ds):
    output = {}
    for elem in ds:
        if elem.VR == "SQ":
            output[elem.keyword] = [convert_to_dict(item) for item in elem.value]
        else:
            output[elem.keyword] = convert_value(elem.value)
    return output


def convert_value(value):
    if isinstance(value, pydicom.valuerep.PersonName):
        return str(value)
    elif isinstance(value, (pydicom.valuerep.DSfloat, pydicom.valuerep.IS)):
        return float(value)
    elif isinstance(value, bytes):
        return value.decode(errors="ignore")
    elif isinstance(value, pydicom.multival.MultiValue):
        return [convert_value(v) for v in value]
    elif isinstance(value, pydicom.dataset.Dataset):
        return convert_to_dict(value)
    else:
        return value


def get_mapping(attribute_number):
    attributes = attribute_number.split(",")
    return hex(int(attributes[0], 16)), hex(int(attributes[1], 16))


def process_text(measurement):
    measurement_concept = measurement[get_mapping("0040,a043")][0][
        get_mapping("0008,0104")
    ]
    measurement_value = measurement[get_mapping("0040,a160")]
    return measurement_concept, measurement_value


def process_code(measurement):
    measurement_concept = measurement[get_mapping("0040,a043")][0][
        get_mapping("0008,0104")
    ]
    measurement_value = measurement[get_mapping("0040,a168")][0][
        get_mapping("0008,0104")
    ]
    return measurement_concept, measurement_value


def process_pname(measurement):
    measurement_concept = measurement[get_mapping("0040,a043")][0][
        get_mapping("0008,0104")
    ]
    measurement_value = measurement[get_mapping("0040,a123")]
    return measurement_concept, measurement_value


def process_num(measurement):
    measurement_concept = measurement[get_mapping("0040,a043")][0][
        get_mapping("0008,0104")
    ]

    # Access the Measured Value Sequence
    if get_mapping("0040,a300") in measurement:
        measured_value_sequence = measurement[get_mapping("0040,a300")]

        if measured_value_sequence:
            measured_value = measured_value_sequence[0]

            # Get the numeric value
            if get_mapping("0040,a30a") in measured_value:
                measurement_value = measured_value[get_mapping("0040,a30a")].value
            else:
                measurement_value = None

            # Get the measurement units
            if get_mapping("0040,08ea") in measured_value:
                measurement_units = measured_value[get_mapping("0040,08ea")][0][
                    get_mapping("0008,0104")
                ].value
            else:
                measurement_units = None
        else:
            measurement_value = None
            measurement_units = None
    else:
        measurement_value = None
        measurement_units = None

    return measurement_concept, measurement_value, measurement_units


def process_datetime(measurement):
    measurement_concept = measurement[get_mapping("0040,a043")][0][
        get_mapping("0008,0104")
    ]
    measurement_value = measurement[get_mapping("0040,a120")]
    return measurement_concept, measurement_value


def process_date(measurement):
    measurement_concept = measurement[get_mapping("0040,a043")][0][
        get_mapping("0008,0104")
    ]
    measurement_value = measurement[get_mapping("0040,a121")]
    return measurement_concept, measurement_value


def process_time(measurement):
    measurement_concept = measurement[get_mapping("0040,a043")][0][
        get_mapping("0008,0104")
    ]
    measurement_value = measurement[get_mapping("0040,a122")]
    return measurement_concept, measurement_value


def process_uidref(measurement):
    measurement_concept = measurement[get_mapping("0040,a043")][0][
        get_mapping("0008,0104")
    ]
    measurement_value = measurement[get_mapping("0040,a124")]
    return measurement_concept, measurement_value


def process_container_type(container, report):
    container_type = container[get_mapping("0040,a040")]

    if container_type.value != "CONTAINER":
        return

    container_concept = container[get_mapping("0040,a043")][0][get_mapping("0008,0104")]

    report[container_concept.value] = {}

    if get_mapping("0040,a730") not in container:
        return container_concept.value, report

    container_sequence = container[get_mapping("0040,a730")]

    for item in container_sequence:

        measurement_report_content_type = item[get_mapping("0040,a040")].value

        if measurement_report_content_type == "TEXT":
            measurement_concept, measurement_value = process_text(item)
            report[container_concept.value][
                measurement_concept.value
            ] = measurement_value.value

        elif measurement_report_content_type == "CODE":
            measurement_concept, measurement_value = process_code(item)
            report[container_concept.value][
                measurement_concept.value
            ] = measurement_value.value

        elif measurement_report_content_type == "PNAME":
            measurement_concept, measurement_value = process_pname(item)
            report[container_concept.value][measurement_concept.value] = convert_value(
                measurement_value.value
            )

        elif measurement_report_content_type == "NUM":
            measurement_concept, measurement_value, measurement_units = process_num(
                item
            )

            if measurement_value is not None and measurement_units is not None:
                report[container_concept.value][measurement_concept.value] = {
                    "value": measurement_value,
                    "units": measurement_units,
                }
            elif measurement_value is not None:
                report[container_concept.value][measurement_concept.value] = {
                    "value": measurement_value
                }

        elif measurement_report_content_type == "DATETIME":
            measurement_concept, measurement_value = process_datetime(item)
            report[container_concept.value][
                measurement_concept.value
            ] = measurement_value.value

        elif measurement_report_content_type == "DATE":
            measurement_concept, measurement_value = process_date(item)
            report[container_concept.value][
                measurement_concept.value
            ] = measurement_value.value

        elif measurement_report_content_type == "TIME":
            measurement_concept, measurement_value = process_time(item)
            report[container_concept.value][
                measurement_concept.value
            ] = measurement_value.value

        elif measurement_report_content_type == "UIDREF":
            measurement_concept, measurement_value = process_uidref(item)
            report[container_concept.value][
                measurement_concept.value
            ] = measurement_value.value

        elif measurement_report_content_type == "CONTAINER":
            container_report = {}
            container_concept_found, container_value_found = process_container_type(
                item, container_report
            )
            report[container_concept.value][container_concept_found] = (
                container_value_found[container_concept_found]
            )

    return container_concept.value, report


def parse_age(age_str):
    if age_str is None:
        return None
    elif age_str.endswith("Y"):
        return int(age_str[:-1])
    return None


def add_if_exists(ds, field):
    """Checks if a field exists in a DICOM dataset and returns its value, otherwise returns 0.

    Args:
        ds (pydicom.dataset.FileDataset): The DICOM dataset to search for the field.
        field (str): The name of the field to search for.

    Returns:
        The value of the field if it exists in the dataset, otherwise 0.
    """
    if field in ds:
        return ds[field].value
    else:
        return None


def convert_dicom_to_json(ds):
    report = {}
    report["PatientID"] = add_if_exists(ds, "PatientID")
    report["PatientBirthDate"] = add_if_exists(ds, "PatientBirthDate")
    report["AccessionNumber"] = add_if_exists(ds, "AccessionNumber")
    report["SOPInstanceUID"] = add_if_exists(ds, "SOPInstanceUID")
    report["PatientSex"] = add_if_exists(ds, "PatientSex")
    report["PatientAge"] = parse_age(add_if_exists(ds, "PatientAge"))
    report["PatientSize"] = add_if_exists(ds, "PatientSize")
    report["PatientWeight"] = add_if_exists(ds, "PatientWeight")
    report["EthnicGroup"] = add_if_exists(ds, "EthnicGroup")
    report["StudyTime"] = add_if_exists(ds, "StudyTime")
    report["StudyDate"] = add_if_exists(ds, "StudyDate")
    report["StudyDescription"] = add_if_exists(ds, "StudyDescription")
    report["StudyInstanceUID"] = add_if_exists(ds, "StudyInstanceUID")
    report["Modality"] = add_if_exists(ds, "Modality")
    report["InstitutionName"] = add_if_exists(ds, "InstitutionName")
    report["StationName"] = add_if_exists(ds, "StationName")
    report["Manufacturer"] = add_if_exists(ds, "Manufacturer")
    report["ManufacturerModelName"] = add_if_exists(ds, "ManufacturerModelName")
    report["SoftwareVersions"] = add_if_exists(ds, "SoftwareVersions")

    _, report = process_container_type(ds, report)
    return report
