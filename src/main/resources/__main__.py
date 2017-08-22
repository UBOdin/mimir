from sys import argv
import Plot
import Test

command = argv[1];

if command == "test":
  Test.run(argv);
elif command == "plot":
  Plot.run(argv)
else:
  raise Exception("Invalid Command: '"+command+"'")

