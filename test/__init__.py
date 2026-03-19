def get_test_file(filename=''):
    import os
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', filename)


def check_helper(args, capsys, expected_exit_value):
    from warcio.cli import main
    exit_value = None
    try:
        main(args=args)
    except SystemExit as e:
        exit_value = e.code
    finally:
        assert exit_value == expected_exit_value

    return capsys.readouterr().out
