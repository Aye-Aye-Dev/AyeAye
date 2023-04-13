import re


def s3_pattern_match(key_pattern):
    """
    The S3 API can only filter by the start of the file/object name. The rest is done with
    a regular expression.

    @param key_pattern: (str)
        A key with a wildcard in it. e.g. 'my_directory/sub_directory/*.csv'

    @return: prefix, re_matcher (str, func)
        func takes a single argument, the full path and returns True if it matches the pattern.
    """
    assert "*" in key_pattern

    # regex safety. This does restrict some legitimate object paths hence the type of exception
    # raised.
    reg_ex_reserved = "?\()!^$+{}[]|<"
    for char in reg_ex_reserved:
        if char in key_pattern:
            msg = f"Sorry, object pattern not supported. It can't have '{reg_ex_reserved}' in it."
            raise NotImplementedError(msg)

    prefix = key_pattern.split("*", 1)[0]

    # make it into a regex
    expression = key_pattern.replace(".", "\.").replace("*", ".*")
    pattern = re.compile(expression)

    def check_file_path(full_path):
        return pattern.fullmatch(full_path) is not None

    return prefix, check_file_path
