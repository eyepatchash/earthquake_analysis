import os
import glob


#This converts a single csv file from the spark output 
def outputfile(output_dir,merged_file):

    # Find the part file. This assumes there's only one part file in the directory.
    part_file = glob.glob(f"{output_dir}/part-*.csv")[0]

    # Move and rename the part file to your desired CSV file
    os.rename(part_file, merged_file)

    # os.rmdir(output_dir)
