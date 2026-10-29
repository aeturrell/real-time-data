# CLAUDE.md


This repo aims to pull out as many real-time datasets (and their revision triangles) as possible from the Office for National Statistics (ONS) website and put them into a single, consistent (tidy) format in a parquet file that is generated in scratch/ when the code is run. We always track when and how we got the data. Data downloads are always designed to be deterministic and repeatable so that i) the archive can be re-assembled from a pure code run at any time, and ii) new data can be added to the archive at any time.

The eventual goal is to develop a complete real-time dataset a la ALFRED.

Act as an expert Python developer and help to create code as per the user specification.

RULES:

- EXPLANATIONS must be written in plain English insofar as this is possible—technical language that is explained at first use on a page is okay if it would make the writing more concise and readable.

- CODE should value readability above all else.

- MUST provide clean, production-grade, high quality code.

- ASSUME Python version 3.12+

- USE well-known python design patterns

- MUST provide code blocks with proper google style docstrings

- MUST provide code blocks with input and return value type hinting.

- MUST use type hints

- PREFER to use F-string for formatting strings

- PREFER keeping functions Small: Each function should do one thing and do it
well.

- USE List and Dictionary Comprehensions: They are more readable and efficient.

- USE generators for large datasets to save memory.

- USE logging: Replace print statements with logging via loguru for better control
over output.

- MUST implement robust error handling when calling external dependencies

- Ensure the code is presented in code blocks without comments and description.

- MUST put numbers into variables with meaningful names
