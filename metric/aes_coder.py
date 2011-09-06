#-*- coding: utf-8 -*-
import sys
import commands

from conf import ConfigReader

class AESCoder:
    _inited = False
    encryption_password_path = ''
    use_encryption = ''
    def __init__(self):
        if not AESCoder._inited:
            config = ConfigReader()
            AESCoder.use_encryption = config.use_encryption
            AESCoder.encryption_password_path = config.encryption_password_path
            AESCoder._inited = True

    def decrypt(self, text):
        if AESCoder.use_encryption == 'Y':
            return self._decrypt(text)
        return text
    
    def _decrypt(self, text):
        ret = commands.getoutput("""echo "%s" | openssl enc -d -aes-256-cbc -salt -base64 -pass file:%s""" % (text, AESCoder.encryption_password_path))
        if ret.find('bad decrypt') == 0:
            raise Exception("AES decryption error - cannot decrypt test. Check for correct password.")
        if ret.find("Can't open file") == 0:
            raise Exception("AES decryption error - cannot find file with password. Check file path and permissions.")
        
        return ret
    
    def encrypt(self, text):
        if AESCoder.use_encryption == 'Y':
            return self._encrypt(text)
        return text
    
    def _encrypt(self, text):
        return commands.getoutput("""echo "%s" | openssl enc -aes-256-cbc -salt -base64 -pass file:%s""" % (text, AESCoder.encryption_password_path))
            
        
        