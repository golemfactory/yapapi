from yapapi.utils import explode_dict


def test_explode_dict():
    assert explode_dict(
        {
            "root_field": 1,
            "nested.field": 2,
            "nested.obj": {
                "works": "fine",
            },
            "nested.obj.with_array": [
                "okay!",
            ],
            "even.more.nested.field": 3,
            "arrays.0.are": {
                "supported": "too",
            },
            "arrays.1": "works fine",
        }
    ) == {
        "root_field": 1,
        "nested": {
            "field": 2,
            "obj": {
                "works": "fine",
                "with_array": [
                    "okay!",
                ],
            },
        },
        "even": {
            "more": {
                "nested": {
                    "field": 3,
                },
            },
        },
        "arrays": [
            {
                "are": {
                    "supported": "too",
                },
            },
            "works fine",
        ],
    }
