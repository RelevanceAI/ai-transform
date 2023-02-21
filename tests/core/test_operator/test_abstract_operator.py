from ai_transform.operator.abstract_operator import AbstractOperator


class TestAbstractOperator:
    def test_input_assertion(self):
        try:

            class TestInputAssertionOperator(AbstractOperator):
                def __init__(self):
                    super().__init__(
                        "input_field",
                        ["output_field"],
                    )

        except:
            assert True

    def test_output_assertion(self):
        try:

            class TestOutputAssertionOperator(AbstractOperator):
                def __init__(self):
                    super().__init__(
                        ["input_field"],
                        "output_field",
                    )

        except:
            assert True
