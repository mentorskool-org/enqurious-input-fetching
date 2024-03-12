import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# fetch credentials
database_uri = os.environ.get("database_uri")
database_password = os.environ.get("database_password")
database_name = os.environ.get("database_name")
database_user = os.environ.get("database_user")


# Establish database connection
def connect_to_database():
    return psycopg2.connect(
        database=database_name,
        user=database_user,
        password=database_password,
        host=database_uri,
        port=5432,
    )


# Function to fetch project details
def get_project_details(project_ids_ls, connection):
    cursor = connection.cursor()
    # Get all the project info
    cursor.execute(
        f"""SELECT id, name from projects where id in %s""", (tuple(project_ids_ls),)
    )
    result = cursor.fetchall()

    return [{"id": record[0], "name": record[1]} for record in result]


def get_input_details(project_id, connection):
    cursor = connection.cursor()
    # Fetch the inputs
    cursor.execute(
        f"""
        SELECT id, skills, payload, activity_id, type, project_id, "order"
        FROM inputs
        WHERE project_id='{project_id}'
        ORDER BY "order";
    """
    )

    result = cursor.fetchall()
    return [
        {
            "id": record[0],
            "skills": record[1],
            "payload": record[2],
            "activity_id": record[3],
            "type": record[4],
            "project_id": record[5],
            "order": record[6],
        }
        for record in result
    ]


def get_activity_details(project_id, connection):
    cursor = connection.cursor()
    # Now fetch the data of activities
    cursor.execute(
        f"""
        SELECT id, level, position 
        FROM activities 
        WHERE project_id='{project_id}';
    """
    )
    result = cursor.fetchall()

    return [
        {"activity_id": record[0], "level": record[1], "position": record[2]}
        for record in result
    ]


# Function to fetch options for an input
def fetch_options(data, options):
    option_data = {
        f"Option {i+1}": option["content"] for i, option in enumerate(options)
    }
    data.update(option_data)
    return data


def main():
    # Take inputs from user
    domain = input("Enter the domain of your inputs:")
    project_ids = [id.strip() for id in input("Enter the project_ids:").split(",")]

    # Establish connection to database
    connection = connect_to_database()
    projects = get_project_details(project_ids, connection)

    # # Iterate over the loops and fetch all the inputs info
    inputs = []
    activities = []
    for project in projects:
        inputs += get_input_details(project["id"], connection)
        activities += get_activity_details(project["id"], connection)

    # Now close the connection
    connection.close()

    #  Convert data into dataframe
    inputs_df = pd.json_normalize(inputs)
    activities_df = pd.DataFrame(activities)

    inputs_activity_df = inputs_df.merge(activities_df, how="inner", on="activity_id")

    # Filter the data based on input type, filter out only checkbox and mcq
    mcq_checkbox_input = inputs_activity_df[
        inputs_activity_df["type"].isin(["mcq", "checkbox"])
    ].sort_values(by=["project_id", "position", "order"])
    
    # Derive image_input, to filter out the inputs that contains images in options
    mcq_checkbox_input["image_input"] = mcq_checkbox_input.apply(
        lambda x: any("image" in option.keys() for option in x["payload.options"]),
        axis=1,
    )
    mcq_checkbox_text_inputs = mcq_checkbox_input[
        ~mcq_checkbox_input["image_input"]
    ].reset_index(drop=True)


    all_inputs_df = pd.DataFrame(
        columns=[
            "Name",
            "Description",
            "Correct Answer",
            "Level",
            "Tags",
            "Insight Tags",
            "Solving Time In Minutes",
            "Score",
            "Penalty",
            "Option 1",
            "Option 2",
            "Option 3",
            "Option 4",
            "Option 5",
            "Option 6",
            "Option 7",
            "Option 8",
            "Option 9",
            "Option 10",
        ]
    )

    level_mapping = {"Beginner": "Easy", "Intermediate": "Medium", "Advanced": "Hard"}

    def find_correct_answer(options, correct_answer_id: list):
        return [index+1 for index, option in enumerate(options) if option["id"] in correct_answer_id]

    all_inputs_data = []
    previous_activity_id = ""
    sequence = 0
    for _, row in mcq_checkbox_text_inputs.iterrows():
        single_input_data = {}

        current_activity_id = row["activity_id"]
        project_id_prefix = row["project_id"][:5]
        activity_id_prefix = current_activity_id[:5]

        # 1. Name
        if current_activity_id != previous_activity_id:
            sequence = 1
        else:
            sequence += 1

        single_input_data["Name"] = (
            f"{domain.title()}-{project_id_prefix}-{activity_id_prefix}-0{sequence}"
        )

        previous_activity_id = current_activity_id

        # 2. Description
        single_input_data["Description"] = row["payload.question"]

        # 3. Correct Answer
        if not pd.isna(row["payload.correct_option_id"]):
            single_input_data['Correct Answer'] = ",".join(map(str, find_correct_answer(row["payload.options"], row['payload.correct_option_id'])))
        else:
            # print(find_correct_answer(row["payload.options"], row['payload.correct_option_ids']))
            single_input_data['Correct Answer'] = ",".join(map(str, find_correct_answer(row["payload.options"], row['payload.correct_option_ids'])))

        # 4. Level
        single_input_data["Level"] = level_mapping[row["level"]]

        # 5. Tags
        single_input_data["Tags"] = ", ".join(row["skills"])

        # 6. Score
        single_input_data["Score"] = (len(row["skills"]) * 10) // len(row["skills"]) if len(row["skills"]) > 1 else 0

        # 7. Now fetch the options dynamically
        single_input_data = fetch_options(single_input_data, row["payload.options"])

        all_inputs_data.append(single_input_data)

    inputs_df_doSelect_format = pd.concat(
        [all_inputs_df, pd.DataFrame(all_inputs_data)]
    )

    # Now check the Description and Correct Answer Fields, both are mandatory, so filter out the blank values
    inputs_df_doSelect_format = inputs_df_doSelect_format[
        inputs_df_doSelect_format["Description"] != ""
    ]

    # Similarly filter out the blank values in Correct Answer
    inputs_df_doSelect_format = inputs_df_doSelect_format[
        inputs_df_doSelect_format["Correct Answer"] != ""
    ]

    # assign default value as 1
    inputs_df_doSelect_format["Solving Time In Minutes"] = 1

    inputs_df_doSelect_format.to_csv("Mcq_and_checkboxes_inputs.csv", index=False)


main()
