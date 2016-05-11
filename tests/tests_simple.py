import unittest
import unittest.mock as mock
import os
import sys
sys.path.insert(0, os.path.abspath('..'))

import wolo.helper as helper

class TestHelperFunctions(unittest.TestCase):
    
    def test_pretty_print_index(self):
        test_index = (1, 4, "p3", 3)
        exspected_output = "[1][4][p3][3]"
        output = helper.pretty_print_index(test_index)
        self.assertEqual(output, exspected_output)

    def test_cut_or_pad_without_master_longer(self):
        test_master = range(0,5)
        test_slave = test_master[:-2]
        exspected_output = [(0, 0), (1, 1), (2, 2), (3, None), (4, None)]
        output = list(helper.cut_or_pad(test_master, test_slave))
        self.assertEqual(exspected_output, output)

    def test_cut_or_pad_without_master_shorter(self):
        test_slave = range(0,5)
        test_master = test_slave[:-2]
        exspected_output = [(0, 0), (1, 1), (2, 2)]
        output = list(helper.cut_or_pad(test_master, test_slave))
        self.assertEqual(exspected_output, output)

    def test_cut_or_pad_without_enum(self):
        test_master = test_slave = range(1,4)
        exspected_output = [(0, 1, 1), (1, 2, 2), (2, 3, 3)]
        output = list(helper.cut_or_pad(test_master, test_slave, enum=True))
        self.assertEqual(exspected_output, output)

import wolo.parameters as parameters
import hashlib
class TestParamterDefinitions(unittest.TestCase):

    def test_simple_parameter(self):
        test_parameter = parameters.Parameter("test", 4)
        exspected_log_value = 4
        self.assertEqual(test_parameter._log_value, exspected_log_value)

    def test_simple_parameter_manual_log_value(self):
        test_parameter = parameters.Parameter("test", 4, 5)
        exspected_log_value = 5
        self.assertEqual(test_parameter._log_value, exspected_log_value)

    @mock.patch("wolo.parameters.os.path.isfile", side_effect=lambda x: True)
    @mock.patch("wolo.parameters.os.path.getmtime", side_effect=lambda x: 11111)
    @mock.patch("wolo.parameters.os.path.abspath", side_effect=lambda x: x)
    def test_file_parameter(self, abspath_mock, getmtime_mock, isfile_mock):
        test_file = parameters.File("test", "../test_dir/test")
        self.assertEqual(test_file.name, "test")
        abspath_mock.assert_called_with("../test_dir/test")
        self.assertEqual(test_file.value, "../test_dir/test")
        self.assertEqual(test_file._log_value, ["../test_dir/test", 11111])

    @mock.patch("wolo.parameters.os.path.isfile", side_effect=lambda x: True)
    @mock.patch("wolo.parameters.os.path.getmtime", side_effect=lambda x: 11111)
    def test_file_parameter_changed(self, getmtime_mock, isfile_mock):
        test_file = parameters.File("test", "../test_dir/test")
        getmtime_mock.side_effect = lambda x: 22222
        self.assertEqual(test_file._get_mod_date(), 22222)
        self.assertTrue(test_file.changed())

    @mock.patch("wolo.parameters.os.path.isfile", side_effect=lambda x: False)
    @mock.patch("wolo.parameters.os.path.getmtime", side_effect=lambda x: 11111)
    @mock.patch("wolo.parameters.os.path.abspath", side_effect=lambda x: x)
    @mock.patch("wolo.parameters.os.makedirs")
    @mock.patch("wolo.parameters.open")
    def test_file_parameter_autocreate(self, open_mock, makedirs_mock, abspath_mock, getmtime_mock, isfile_mock):
        test_file = parameters.File("test", "../test_dir/test", autocreate=True)
        makedirs_mock.assert_called_with("../test_dir", exist_ok=True)
        open_mock.assert_called_with("../test_dir/test", 'a')

    @mock.patch("wolo.parameters.inspect.getsource", side_effect=lambda x: x)
    def test_source_parameter(self, getsource_mock):
        test_object = parameters.Source("this is a test")
        getsource_mock.assert_called_with("this is a test")
        self.assertEqual(test_object.name, "this is a test")
        self.assertEqual(test_object._log_value, hashlib.md5("this is a test".encode('utf-8')).hexdigest())

    @mock.patch("wolo.parameters.inspect.getsource", side_effect=lambda x: x)
    def test_source_parameter_changed(self, getsource_mock):
        test_object = parameters.Source("this is a test")
        getsource_mock.side_effect = lambda x: "this is changed test"
        self.assertTrue(test_object.changed())

    @mock.patch("wolo.parameters.Source.__init__")
    def test_self_paramter(self, source_mock):
        test_object = parameters.Self(list())
        source_mock.assert_called_with(object=list, name="Self")













if __name__ == '__main__':
    unittest.main()