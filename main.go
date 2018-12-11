package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"
)

var flagUser string
var flagPass string

type Document struct {
	ID   string
	Name string
	Size int
}

type DocumentDAO struct {
	ID   string
	Name string
	Size int
	Path string
}

type User struct {
	ID    string
	Name  string
	Email string
}

var docs map[string]DocumentDAO
var users map[string]User


func UploadDocument(file []byte, name string) string {

	f, err := os.OpenFile("./storageFile/"+name, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("successfully saved")
		return ""
	}
	defer f.Close()
	r := bytes.NewReader(file)
	io.Copy(f, r)

	return name

}

func GetDocumentById(w http.ResponseWriter, r *http.Request) {
	//var docs []Document
	docs = loadDocuments(docs)
	vars := mux.Vars(r)

	w.Header().Set("Content-Type", "application/json")
	var doc DocumentDAO
	for _, v := range docs {
		if v.ID == vars["id"] {
			doc = v

		}
	}

	if documentInArray(vars["id"], docs) != "" {
		json.NewEncoder(w).Encode(parseDocument(doc))
	} else {
		http.Error(w, "", http.StatusNotFound)

	}

}

func documentInArray(a string, list map[string]DocumentDAO) string {
	for _, b := range list {
		if b.ID == a {
			return b.Name
		}
	}
	return ""
}

func GetDocuments() map[string]DocumentDAO {
	var docs map[string]DocumentDAO
	docs = make(map[string]DocumentDAO)
	docs = loadDocuments(docs)
	return docs

	//w.Header().Set("Content-Type", "application/json")

	//json.NewEncoder(w).Encode(parseDocuments(docs))
	//json.NewEncoder(w).Encode(docs)

}

func loadDocuments(docs map[string]DocumentDAO) map[string]DocumentDAO {
	root := "./storageFile/"
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.Name() != "storageFile" {
			id, error := hash_file_md5(path)
			if error == nil {
				//*docs = append(*docs,
				//	Document{ID: id, Name: info.Name(), Size: int(info.Size())})
				docs[id] = DocumentDAO{ID: id, Name: info.Name(), Size: int(info.Size()), Path: path}
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return docs
}

func DeleteDocuments(id string) string {
	//var docs []Document
	docs = make(map[string]DocumentDAO)
	docs = loadDocuments(docs)
	result := documentInArray(id, docs)
	if result != "" {
		deleteDocument(id)
		return docs[id].Name
	} else {
		return ""
	}

}

func deleteDocument(docId string) bool {
	root := "./storageFile/"
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.Name() != "storageFile" {
			id, error := hash_file_md5(path)
			if error == nil {
				if id == docId {
					os.Remove(path)
				}
			}
		}
		return nil
	})
	if err != nil {
		return true
	} else {
		return false
	}
}

func hash_file_md5(filePath string) (string, error) {
	var returnMD5String string
	file, err := os.Open(filePath)
	if err != nil {
		return returnMD5String, err
	}
	defer file.Close()
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return returnMD5String, err
	}
	hashInBytes := hash.Sum(nil)[:16]
	returnMD5String = hex.EncodeToString(hashInBytes)
	return returnMD5String, nil
}

func basicAuth(h http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()

		if flagUser != user || flagPass != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			http.Error(w, "Unauthorized.", http.StatusUnauthorized)
			return
		}

		h.ServeHTTP(w, r)
	})
}

func use(h http.HandlerFunc, middleware ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	/*for _, m := range middleware {
		h = m(h)
	}*/ //esto da cabeceras de seguridad
	return h
}

func ServeDocuments(w http.ResponseWriter, r *http.Request) {
	//var docs []Document
	docs = loadDocuments(docs)
	vars := mux.Vars(r)
	var docPath string
	//w.Header().Set("Content-Type", "application/octet-stream")
	//w.Header().Set("Content-Disposition", "attachment")

	if documentInArray(vars["id"], docs) != "" {
		docPath = serveDocument(vars["id"])
		if docPath != "" {

			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Disposition", "attachment; filename="+docs[vars["id"]].Name)
			http.ServeFile(w, r, docPath)
		} else {
			http.Error(w, "", http.StatusInternalServerError)
		}
	} else {
		http.Error(w, "", http.StatusNotFound)

	}

}

func serveDocument(docId string) string {
	root := "./storageFile/"
	var docPath string
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.Name() != "storageFile" {
			id, error := hash_file_md5(path)
			if error == nil {
				if id == docId {
					docPath = path
				}
			}
		}
		return nil
	})

	return docPath

}

func parseDocuments(dao map[string]DocumentDAO) []Document {
	var d []Document
	d = make([]Document, 0)
	for _, data := range dao {
		d = append(d, Document{ID: data.ID, Name: data.Name, Size: data.Size})
	}
	return d
}

func parseDocument(data DocumentDAO) Document {
	return Document{ID: data.ID, Name: data.Name, Size: data.Size}

}
