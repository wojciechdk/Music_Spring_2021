from runipy.notebook_runner import NotebookRunner
from nbformat import read
from nbformat import write

#%%
path = r'/data/work/shared/s001284/Music_Project/test.ipynb'
notebook = read(open(path, 'r', encoding='utf-8'), 3)
r = NotebookRunner(notebook)
r.run_notebook()

write(r.nb, open(path, 'w'), 3)

