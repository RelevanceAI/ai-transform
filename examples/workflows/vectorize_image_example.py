import argparse

from typing import Callable, List, Optional

from workflows_core.api.client import Client
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.workflow.helpers import decode_workflow_token
from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.random import Document

import requests
from PIL import Image
from sentence_transformers import SentenceTransformer


class VectorizeImageOperator(AbstractOperator):
    def __init__(
        self,
        image_fields: str,
        model: str = "clip-ViT-B-32",
        alias: Optional[str] = None,
    ):

        self._model = SentenceTransformer(model)

        self._image_fields = image_fields
        self._alias = model.replace("/", "-") if alias is None else alias
        self._output_fields = [
            f"{image_field}_{self._alias}_vector_" for image_field in image_fields
        ]

        super().__init__(
            input_fields=self._image_fields,
            output_fields=self._output_fields,
        )

    def _convert_imageurl_to_pil_rgb(self, imageurl: str) -> Image:
        """
        Convert a Image URL to a PIL Image
        """
        img = Image.open((requests.get(imageurl, stream=True).raw))
        if img.mode == "RGB":
            return img
        if img.mode == "RGBA":
            background = Image.new("RGB", img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[3])
            return background
        elif img.mode == "L":
            return img.convert("RGB")
        else:
            return img.convert("RGB")

    def _image_encoding(self, image_urls: List[str]) -> List[List[float]]:

        return self._model.encode(
            [self._convert_imageurl_to_pil_rgb(imageurl) for imageurl in image_urls]
        ).tolist()

    def transform(self, documents: List[Document]) -> List[Document]:
        """
        Main transform function
        """
        for field_index in range(len(self._input_fields)):
            batch = [
                document[self._image_fields[field_index]] for document in documents
            ]
            vectors = self._image_encoding(batch)

            for index in range(len(vectors)):
                documents[index][self._output_fields[field_index]] = vectors[index]

        return documents


def execute(token: str, logger: Callable, *args, **kwargs):
    config = decode_workflow_token(token)

    token = config.get("token")
    dataset_id = config.get("dataset_id")
    job_id = config.get("job_id")
    image_fields = config.get("image_fields")
    alias = config.get("alias", None)
    chunksize = config.get("chunksize", 20)

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    operator = VectorizeImageOperator(
        image_fields=image_fields,
        alias=alias,
    )

    filters = []
    for field in image_fields:
        filters = dataset[field].exists()
    filters = [{"filter_type": "or", "condition_value": filters}]

    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        chunksize=chunksize,
        select_fields=image_fields,
        filters=filters,
    )

    workflow = AbstractWorkflow(
        engine=engine,
        job_id=job_id,
    )
    workflow.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Vectorize Text Workflow.")
    parser.add_argument(
        "token",
        type=str,
        help="a base64 encoded token that contains parameters for running the workflow",
    )
    args = parser.parse_args()
    execute(args.token, print)
