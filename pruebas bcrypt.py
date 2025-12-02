import bcrypt

# from gestion_user.control_usuarios import aut_user


txt = "1234"
pw = txt.encode("utf-8")
sal = bcrypt.gensalt()

encript = bcrypt.hashpw(pw, sal)

# contrasenia_encript = bytes(data_user('JDORANTES')['passw'][0], 'utf-8')

print(encript)

# # result = bcrypt.checkpw(bytes('18329114', 'utf-8'), contrasenia_encript)

# # print(result)

# print(aut_user('JDORANTES', '18329114'))


# from scripts.oCelulas import DataCelulas

# print(DataCelulas().sobres_por_entregar())
