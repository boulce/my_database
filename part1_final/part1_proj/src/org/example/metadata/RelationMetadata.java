package org.example.metadata;

public class RelationMetadata {
    private String relationName;
    private int numberOfAttributes;

    public RelationMetadata(String relationName, int numberOfAttributes, String storageOrganization, String location) {
        this.relationName = relationName;
        this.numberOfAttributes = numberOfAttributes;
        this.storageOrganization = storageOrganization;
        this.location = location;
    }

    private String storageOrganization;
    private String location;

    public String getRelationName() {
        return relationName;
    }

    public void setRelationName(String relationName) {
        this.relationName = relationName;
    }

    public int getNumberOfAttributes() {
        return numberOfAttributes;
    }

    public void setNumberOfAttributes(int numberOfAttributes) {
        this.numberOfAttributes = numberOfAttributes;
    }

    public String getStorageOrganization() {
        return storageOrganization;
    }

    public void setStorageOrganization(String storageOrganization) {
        this.storageOrganization = storageOrganization;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}
