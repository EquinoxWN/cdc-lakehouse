from cdc_lakehouse.e2e import annotation


def test_annotation_keeps_a_multiline_report_on_one_line():
    line = annotation("notice", "CDC check", "| a | 100% |\n| b | ok |")
    assert line == "::notice title=CDC check::| a | 100%25 |%0A| b | ok |"
    assert "\n" not in line


def test_annotation_title_cannot_end_the_command_early():
    assert annotation("error", "a,b::c", "x").startswith("::error title=a%2Cb: :c::")
