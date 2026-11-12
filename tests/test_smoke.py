import unittest
from al01.core import Individual, World, Parser, CLError

class FoundationSmoke(unittest.TestCase):
    def test_basic_run(self):
        individual = Individual(World(10))
        self.assertEqual(individual.run(20), 20)
        self.assertEqual(individual.state['tick'], 20)
    def test_cl_parse(self):
        self.assertEqual(Parser('RETURN(1);').block()[0]['kind'], 'return')
    def test_invalid_cl_rejected(self):
        with self.assertRaises(CLError):
            Parser('!not-valid')

if __name__ == '__main__': unittest.main()
