from FileDownloader.Downloader import Downloader
from ftplib import FTP

class FTPFileDownloader(Downloader):
    def __init__(self):
        super().__init__()


    def download(self, server, username, password, serverPath, filename):
        print('Downloading file: ' + filename);
        ftp = FTP(server)
        ftp.login(username, password)
        ftp.cwd(serverPath)

        localFile = open(filename, 'wb')
        ftp.retrbinary('RETR ' + filename, localFile.write, 1024)

        ftp.quit()
        localFile.close()

