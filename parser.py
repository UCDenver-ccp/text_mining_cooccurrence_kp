import os
import csv
import json


def get_attribute_object(blob, atrribute_type_id) -> dict:
    for obj in blob:
        if obj['attribute_type_id'] == atrribute_type_id:
            return obj


def get_attribute_list(blob, attribute_type_id) -> list:
    object_list = []
    for obj in blob:
        if obj['attribute_type_id'] == attribute_type_id:
            object_list.append(obj)
    return object_list


def load_nodes(file_path) -> dict:
    nodes_data = {}
    with open(file_path, 'r') as file_handle:
        reader = csv.reader(file_handle, delimiter='\t')
        for row in reader:
            nodes_data[row[0]] = (row[1], row[2])
    return nodes_data


def load_data(data_folder):
    entity_dict = load_nodes(os.path.join(data_folder, "cooccurrence_nodes.tsv"))
    edges_file_path = os.path.join(data_folder, "cooccurrence_edges.tsv")
    with open(edges_file_path, 'r') as file_handle:
        reader = csv.reader(file_handle, delimiter='\t')
        for line in reader:
            subject_parts = line[0].split(':')
            object_parts = line[2].split(':')
            yield {
                "_id": line[3],
                "subject": {
                    "id": line[0],
                    subject_parts[0]: subject_parts[1],
                    "type": entity_dict[line[0]][1].split(':')[-1]
                },
                "association": {
                    "edge_label": line[1].split(':')[-1],
                    "edge_attributes": json.loads(line[-1])
                },
                "object": {
                    "id": line[2],
                    object_parts[0]: object_parts[1],
                    "type": entity_dict[line[2]][1].split(':')[-1]
                },
            }
