import { Student } from "../model/student";
import { Mapper } from "./mapper";

export class CustomMapper implements Mapper {
  data: any;
  constructor(data: any) {
    this.data = data;
  }

  transform(): any[] {
    const students: Student[] = [];
    for (const key in this.data) {
      if (
        key == "tag" &&
        this.data[key] !== "commit" &&
        this.data[key] !== "relation" &&
        this.data[key] !== "begin"
      ) {
        const newObject = this.data["new"];
        students.push(newObject as Student);
      }
    }
    return students;
  }
}
