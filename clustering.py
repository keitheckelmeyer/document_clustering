import csv
import glob
import os
import pickle
import time
import tracemalloc
import PyPDF2
import mammoth
import pandas as pd
from openpyxl import load_workbook
from concurrent.futures import ProcessPoolExecutor as PPE

DIRECTORIES = ['Y:\\', 'Z:\\']

def h_m_s(seconds):
    h = int(seconds / 3600)
    m = int((seconds - (h * 3600)) / 60)
    s = seconds - ((h * 3600) + (m * 60))
    return h, m, s


def get_time_passed(seconds):
    h, m, s = h_m_s(seconds)
    return f"{h} hour(s), {m} minute(s), and {s} second(s)"


def get_files():
    files: list[str] = []
    for d in DIRECTORIES:
        files.extend(glob.glob(d + "\\**\\*", recursive=True))
    print(f"Number of files: {len(files)}")
    return files

class File:

    def __init__(self, file_path):
        self.file_path = file_path
        self.is_file = os.path.isfile(self.file_path)
        self.file_name = os.path.basename(self.file_path) if self.is_file else None
        self.extension = (self.file_name.split("."))[-1] if self.is_file else None
        self.content = None
        self._get_content()

    def _replace_chars(self, contents):
        # TODO: use regex
        replace_list = ['\n', '\t', '     ', '    ', '   ', '  ']
        for replace_ in replace_list:
            contents = contents.replace(replace_, " ")
        return contents

    def _get_file_contents(self):
        # TODO: improve handling for different encodings
        try:
            with open(self.file_path, 'r', errors='namereplace') as f:
                contents = f.read()
            return contents
        except:
            return "ERROR"


    def _get_pdf_contents(self):
        # TODO: expand to perform OCR for PDFs that contain text as images(?)
        # TODO: improve handling for different encodings
        pdf_contents = ''
        try:
            pdf = PyPDF2.PdfReader(self.file_path, strict=False)
            for n in range(len(pdf.pages)):
                pdf_contents += (pdf.pages[n]).extract_text()
            pdf_contents = self._replace_chars(pdf_contents)
            return pdf_contents
        except:
            return "ERROR"

    def _get_txt_contents(self):
        contents = self._get_file_contents()
        contents = self._replace_chars(contents)
        return contents

    def _get_csv_contents(self):
        text = ''
        try:
            contents = self._get_file_contents()
            csvreader = csv.reader(contents)

            # extracting field names through first row
            fields = next(csvreader)
            for field in fields:
                text += field + " "

            # extracting each data row one by one
            for row in csvreader:
                for cell in row:
                    text += cell + " "
            text = self._replace_chars(text)
            return text
        except:
            return "ERROR"

    def _get_docx_contents(self):
        try:
            with open(self.file_path, "rb") as docx_file:
                result = mammoth.extract_raw_text(docx_file)
                text = result.value + " "  # The raw text
                messages = result.messages  # Any messages
            for msg in messages:
                text += msg + " "
            text = self._replace_chars(text)
            return text
        except:
            return "ERROR"

    def _get_doc_contents(self):
        # TODO
        pass

    def _get_xls_contents(self):
        # TODO
        pass

    def _get_xlsx_contents(self):
        # TODO: improve handling for different encodings
        try:
            content = ''
            wb = load_workbook(self.file_path)
            for ws in wb:
                for row in ws.values:
                    for value in row:
                        content += str(value) + " "
            content = self._replace_chars(content)
            return content
        except:
            return "ERROR"

    def _get_content(self):
        d = {
            'pdf': self._get_pdf_contents(),
            'txt': self._get_txt_contents(),
            'docx': self._get_docx_contents(),
            'xlsx': self._get_xlsx_contents(),
            'xlsm': self._get_xlsx_contents(),
            'csv': self._get_csv_contents(),
        }
        if self.extension in d.keys():
            self.content = d[self.extension]
        else:
            self.content = None


def init_file_object(file_path):
    return File(file_path)

def main():
    tracemalloc.start()
    if not os.path.exists('file_obj_list.pickle'):
        files = get_files()
        exts = ['pdf', 'txt', 'docx', 'xlsx', 'xlsm', 'csv', 'html', 'htm']
        files = list(filter(lambda x: (x.split("."))[-1] in exts, files))
        num_files = len(files)

        with PPE() as executor:
            results = list(executor.map(init_file_object, files, chunksize=int(num_files/os.cpu_count() )))

        # results = []
        # for n, file in enumerate(files):
        #     print(f"{n + 1} of {num_files}: {file}")
        #     results.append(init_file_object(file))

        with open("file_obj_list.pickle", 'wb') as f:
            f.write(pickle.dumps(results))

    else:
        with open("file_obj_list.pickle", 'rb') as f:
            results = pickle.loads(f.read())

    current, max_ = tracemalloc.get_traced_memory()
    current_mb = current / 1_048_576
    max_mb = max_ / 1_048_576
    print(f"memory use: {max_mb} MB")

    tracemalloc.stop()

    # vectorize
    # determine number of clusters
    # cluster


if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(f"Time: {get_time_passed(end - start)}")
