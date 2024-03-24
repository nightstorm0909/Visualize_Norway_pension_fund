import nbformat
import sys

from nbconvert.preprocessors import ClearOutputPreprocessor

def remove_outputs(notebook_file):
    with open(notebook_file, "r") as f:
        notebook = nbformat.read(f, as_version=4)
        
    # Create a ClearOutputPreprocessor instance
    preprocessor = ClearOutputPreprocessor()

    # Process the notebook
    processed_notebook = preprocessor.preprocess(notebook, {})[0]

    # Write the processed notebook back to the file
    with open(notebook_file, "w") as f:
        nbformat.write(processed_notebook, f)

if __name__ == "__main__":
    notebook_file = sys.argv[1]
    remove_outputs(notebook_file)
