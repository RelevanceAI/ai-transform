from ai_transform.dataset.dataset import Dataset


class TestContextManager:
    def test_field_children(self, full_dataset: Dataset):
        parent_field = full_dataset["sample_1_vector_"]

        parent_field.add_field_children(["_cluster_.sample_1_vector_.kmeans-1"])
        parent_field.add_field_children(["_cluster_.sample_1_vector_.kmeans-2"])
        parent_field.add_field_children(["_cluster_.sample_1_vector_.kmeans-3"])

        child_field = full_dataset["_cluster_.sample_1_vector_.kmeans-3"]
        child_field.add_field_children(["woah"], recursive=True)

        grandchild_field = full_dataset["woah"]
        grandchild_field.add_field_children(["woah's daughter"], recursive=True)

        field_children = parent_field.list_field_children()

        assert all(
            [
                field_child in field_children
                for field_child in [
                    "woah",
                    "woah's daughter",
                    "_cluster_.sample_1_vector_.kmeans-1",
                    "_cluster_.sample_1_vector_.kmeans-2",
                    "_cluster_.sample_1_vector_.kmeans-3",
                ]
            ]
        )
