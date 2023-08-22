import { Student } from "../model/student";

type Transformer = (data: any) => Student[];

const tagTransformer: Transformer = (data) => {
  const students: Student[] = [];
  for (const key in data) {
    if (
      key == "tag" &&
      data[key] !== "commit" &&
      data[key] !== "relation" &&
      data[key] !== "begin"
    ) {
      const newObject = data["new"];
      students.push(newObject as Student);
    }
  }

  return students;
};

// const anotherTransformer: Transformer = (data) => { ... };

const transformers: Transformer[] = [
  tagTransformer /* , anotherTransformer, ... */,
];

export const transform = (messages: any[]): Student[] => {
  console.log("Transforming messages");
  return ([] as Student[]).concat(
    transformers.flatMap((transformer) => transformer(messages))
  );
};
