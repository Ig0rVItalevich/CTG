import json

from src.logger import logger


def compare_results(expected_result: dict, calculated_result: dict) -> dict:
    count_records = len(expected_result)

    number_of_matches = 0
    matches = {}

    for key in expected_result.keys():
        if expected_result[key] == calculated_result[key]:
            number_of_matches += 1

        matches[key] = {
            "expected": expected_result[key],
            "calculeted": calculated_result[key],
        }

    print("Процент совпадений: {:.2f}".format(number_of_matches / count_records * 100))

    with open("comparison.json", "w") as write_file:
        json.dump(
            matches, write_file, indent=4, ensure_ascii=False, separators=(",", ": ")
        )

    logger.info("Comparison result file created")
