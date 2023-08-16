import PyPDF2, os
def merge_pdfs(pdf_list, output_filename):
    pdf_merger = PyPDF2.PdfMerger()

    for pdf in pdf_list:
        with open(pdf, 'rb') as pdf_file:
            pdf_merger.append(pdf_file)

    with open(output_filename, 'wb') as output_file:
        pdf_merger.write(output_file)


if __name__ == "__main__":

    master_path = r'C:\Users\fabar\OneDrive\Desktop\An Elementary Introduction to Mathematical Finance'
    contents = os.listdir(master_path)

    pdfs = [os.path.join(master_path,x) for x in contents]
    output_file = os.path.join(master_path, "An Elementary Introduction to Mathematical Finance.pdf")
    merge_pdfs(pdfs, output_file)
    print(f"PDFs merged successfully into {output_file}")