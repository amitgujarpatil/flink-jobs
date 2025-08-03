package org.example.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Todo {

    private  int id;

    public Todo(){}

    public String getTodo() {
        return todo;
    }

    public void setTodo(String todo) {
        this.todo = todo;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    private  String todo;
    private  boolean completed;
    private  int userId;

    @Override
    public String toString(){
        return String.format("id: %d, userId: %d, todo: %s",id,userId,todo);
    }

    public List<Todo> loadFromJsonFile(String fileName){
        try{
            InputStream stream = this.getClass().getClassLoader().getResourceAsStream(fileName);
            if (stream == null) {
                throw  new Exception("failed to load");
            }

            ObjectMapper mapper = new ObjectMapper();

            List<Todo> todos = mapper.readValue(stream, new TypeReference<List<Todo>>() {});
            stream.close();
            return todos;
        }catch (Exception e){
           return new ArrayList<Todo>();
        }
    }
}
