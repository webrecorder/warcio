def get_test_file(filename=''):
    import os
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', filename)
