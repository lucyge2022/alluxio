#include <string>

// Abstract class for Payload
class Payload {
public:
    virtual ~Payload() = default;

    // Pure virtual function to get the payload data as a string
    virtual std::string toString() const = 0;

    // Pure virtual function to get the size of the payload
    virtual size_t getSize() const = 0;

    // Optionally, you might want to define other common methods
    virtual void serialize() const = 0;
    virtual void deserialize(const std::string& data) = 0;
};
